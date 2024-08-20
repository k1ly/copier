import { Client } from '@elastic/elasticsearch';
import * as fs from 'fs';
import { TelegramNotification } from '../util/telegram-notification';

export class ElasticCopier {
  private devClient: Client;
  private preprodClient: Client;
  private telegramNotification: TelegramNotification;

  constructor(telegramNotification?: TelegramNotification) {
    this.devClient = new Client({
      cloud: {
        id: process.env.ELASTIC_DEV_CLOUD_ID,
      },
      auth: {
        apiKey: process.env.ELASTIC_DEV_API_KEY,
      },
    });
    this.preprodClient = new Client({
      cloud: {
        id: process.env.ELASTIC_PREPROD_CLOUD_ID,
      },
      auth: {
        apiKey: process.env.ELASTIC_PREPROD_API_KEY,
      },
    });
    this.telegramNotification = telegramNotification;
  }

  private copyIndex = async (indexName: string) => {
    const index = await this.devClient.indices.get({ index: indexName });
    const settings = index[indexName].settings;
    settings.index = Object.fromEntries(
      Object.entries(settings.index).filter(
        ([key]) =>
          !['uuid', 'provided_name', 'creation_date', 'version'].includes(key),
      ),
    );

    await this.preprodClient.indices.create({
      index: indexName,
      aliases: index[indexName].aliases,
      mappings: index[indexName].mappings,
      settings,
    });
  };

  private copyDocuments = async (indexName: string) => {
    let documents = await this.devClient.search({
      index: indexName,
      scroll: '1m',
      body: {
        query: { match_all: {} },
        size: 10000,
      },
    });
    const hits = documents.hits.hits;

    for (
      let scrollId = documents._scroll_id;
      hits.length < (documents.hits.total as any).value;
      scrollId = documents._scroll_id
    ) {
      documents = await this.devClient.scroll({
        scroll_id: scrollId,
        scroll: '1m',
      });
      hits.push(...documents.hits.hits);
    }

    for (const document of hits) {
      await this.preprodClient.index({
        index: indexName,
        document: document._source,
        id: document._id,
      });
    }
  };

  copyIndices = async () => {
    try {
      const indices = process.env.ELASTIC_INDICES.split(',');
      for (const index of indices) {
        await this.copyIndex(index);
        await this.copyDocuments(index);
      }

      console.log('Copied successfully!');
      if (this.telegramNotification)
        this.telegramNotification.sendNotification('Copied successfully!');
    } catch (error) {
      console.log(error);
      if (this.telegramNotification)
        this.telegramNotification.sendNotification(`Error: ${error}`);
    }
  };

  clearIndices = async () => {
    try {
      const indices = await this.preprodClient.cat
        .indices({ format: 'json' })
        .then((indices) =>
          indices.filter((index) =>
            process.env.ELASTIC_INDICES.split(',').includes(index.index),
          ),
        );
      for (const index of indices) {
        await this.preprodClient.indices.delete({ index: index.index });
      }
      console.log('Cleared successfully!');
    } catch (error) {
      console.error(error);
    }
  };

  getCounts = async () => {
    const counts = {};
    try {
      const indices = await this.devClient.cat
        .indices({ format: 'json' })
        .then((indices) =>
          indices.filter((index) =>
            process.env.ELASTIC_INDICES.split(',').includes(index.index),
          ),
        );

      counts[0] = {
        counts: {},
      };

      for (const index of indices) {
        const documents = await Promise.all([
          this.devClient.search({
            index: index.index,
            body: {
              query: {
                match_all: {},
              },
            },
          }),
          this.preprodClient.search({
            index: index.index,
            body: {
              query: {
                match_all: {},
              },
            },
          }),
        ]);
        const documentCounts = documents.map(
          (result) => (result.hits.total as any).value,
        );

        counts[0].counts[index.index] = documentCounts;
      }

      console.log('Counted successfully!');
    } catch (error) {
      console.error(error);
    } finally {
      fs.writeFileSync(
        './out/elastic-counts.json',
        JSON.stringify(counts, null, 2),
      );
    }
  };

  shutdown = async () => {
    await this.preprodClient.close();
    await this.devClient.close();
  };
}
