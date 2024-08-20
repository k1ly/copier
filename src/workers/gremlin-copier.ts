import * as fs from 'fs';
import { driver as gremlin } from 'gremlin';
import { TelegramNotification } from '../util/telegram-notification';

export class GremlinCopier {
  private devClient: gremlin.Client;
  private preprodClient: gremlin.Client;
  private telegramNotification: TelegramNotification;

  constructor(telegramNotification?: TelegramNotification) {
    this.devClient = new gremlin.Client(process.env.GREMLIN_DEV_HOST, {
      authenticator: new gremlin.auth.PlainTextSaslAuthenticator(
        process.env.GREMLIN_DEV_USERNAME,
        process.env.GREMLIN_DEV_PASSWORD,
      ),
      traversalsource: 'g',
      rejectUnauthorized: true,
      mimeType: 'application/vnd.gremlin-v2.0+json',
    });
    this.devClient.open();

    this.preprodClient = new gremlin.Client(process.env.GREMLIN_PREPROD_HOST, {
      authenticator: new gremlin.auth.PlainTextSaslAuthenticator(
        process.env.GREMLIN_PREPROD_USERNAME,
        process.env.GREMLIN_PREPROD_PASSWORD,
      ),
      traversalsource: 'g',
      rejectUnauthorized: true,
      mimeType: 'application/vnd.gremlin-v2.0+json',
    });
    this.preprodClient.open();

    this.telegramNotification = telegramNotification;
  }

  private copyVertices = async () => {
    const verticesQuery = 'g.V()';
    const verticesResult = await this.devClient.submit(verticesQuery);
    const vertices = verticesResult.toArray();

    for (const vertex of vertices) {
      const addVQuery = Object.entries(vertex.properties).reduce(
        (acc, [key, value]) => `${acc}.property('${key}', '${value[0].value}')`,
        `g.addV('${vertex.label}').property('id', '${vertex.id}')`,
      );
      await this.preprodClient.submit(addVQuery);
    }
  };

  private copyEdges = async () => {
    const edgesQuery = 'g.E()';
    const edgesResult = await this.devClient.submit(edgesQuery);
    const edges = edgesResult.toArray();

    for (const edge of edges) {
      const addEQuery = Object.entries(edge.properties).reduce(
        (acc, [key, value]) => `${acc}.property('${key}', '${value}')`,
        `g.V('${edge.outV}').addE('${edge.label}').to(g.V('${edge.inV}')).property('id', '${edge.id}')`,
      );
      await this.preprodClient.submit(addEQuery);
    }
  };

  copyGraph = async () => {
    try {
      await this.copyVertices();
      await this.copyEdges();

      console.log('Copied successfully!');
      if (this.telegramNotification)
        this.telegramNotification.sendNotification('Copied successfully!');
    } catch (error) {
      console.log(error);
      if (this.telegramNotification)
        this.telegramNotification.sendNotification(`Error: ${error}`);
    }
  };

  clearGraph = async () => {
    try {
      const edgesQuery = 'g.E()';
      const edgesResult = await this.preprodClient.submit(edgesQuery);
      const edges = edgesResult.toArray();
      for (const edge of edges) {
        const dropQuery = `g.E('${edge.id}').drop()`;
        await this.preprodClient.submit(dropQuery);
      }

      const verticesQuery = 'g.V()';
      const verticesResult = await this.preprodClient.submit(verticesQuery);
      const vertices = verticesResult.toArray();
      for (const vertex of vertices) {
        const dropQuery = `g.V('${vertex.id}').drop()`;
        await this.preprodClient.submit(dropQuery);
      }

      console.log('Cleared successfully!');
    } catch (error) {
      console.error(error);
    }
  };

  getCounts = async () => {
    const counts = {};
    try {
      const vertexQuery = 'g.V().count()';
      const vertexResults = await Promise.all([
        this.devClient.submit(vertexQuery),
        this.preprodClient.submit(vertexQuery),
      ]);
      const vertexCounts = vertexResults.map((result) => result.first());

      const edgeQuery = 'g.E().count()';
      const edgeResults = await Promise.all([
        this.devClient.submit(edgeQuery),
        this.preprodClient.submit(edgeQuery),
      ]);
      const edgeCounts = edgeResults.map((result) => result.first());

      counts['g'] = { vertexCounts, edgeCounts };

      console.log('Counted successfully!');
    } catch (error) {
      console.error(error);
    } finally {
      fs.writeFileSync(
        './out/gremlin-counts.json',
        JSON.stringify(counts, null, 2),
      );
    }
  };

  shutdown = () => {
    this.devClient.close();
    this.preprodClient.close();
  };
}
