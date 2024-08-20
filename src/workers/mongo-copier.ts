import * as fs from 'fs';
import { MongoClient } from 'mongodb';
import { TelegramNotification } from '../util/telegram-notification';

export class MongoCopier {
  private devClient: MongoClient;
  private preprodClient: MongoClient;
  private telegramNotification: TelegramNotification;

  constructor(telegramNotification?: TelegramNotification) {
    this.devClient = new MongoClient(process.env.MONGO_DEV_URI, {
      auth: {
        username: process.env.MONGO_DEV_USERNAME,
        password: process.env.MONGO_DEV_PASSWORD,
      },
    });
    this.preprodClient = new MongoClient(process.env.MONGO_PREPROD_URI, {
      auth: {
        username: process.env.MONGO_PREPROD_USERNAME,
        password: process.env.MONGO_PREPROD_PASSWORD,
      },
    });
    this.telegramNotification = telegramNotification;
  }

  private copyCollections = async (databaseName: string) => {
    const collections = await this.preprodClient
      .db(databaseName)
      .listCollections()
      .toArray();
    for (const collection of collections) {
      await this.preprodClient
        .db(databaseName)
        .createCollection(collection.name);
    }
  };

  private copyDocuments = async (databaseName: string) => {
    const collections = await this.devClient
      .db(databaseName)
      .listCollections()
      .toArray();
    for (const collection of collections) {
      const documents = await this.devClient
        .db(databaseName)
        .collection(collection.name)
        .find({})
        .toArray();
      if (documents.length > 0) {
        await this.preprodClient
          .db(databaseName)
          .collection(collection.name)
          .insertMany(documents);
      }
    }
  };

  copyDatabases = async () => {
    try {
      const databases = await this.devClient.db().admin().listDatabases();
      for (const database of databases.databases) {
        await this.copyCollections(database.name);
        await this.copyDocuments(database.name);
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

  clearDatabases = async () => {
    try {
      const databases = await this.preprodClient.db().admin().listDatabases();
      for (const database of databases.databases) {
        await this.preprodClient.db(database.name).dropDatabase();
      }
      console.log('Cleared successfully!');
    } catch (error) {
      console.error(error);
    }
  };

  getCounts = async () => {
    const counts = {};
    try {
      const databases = await this.devClient.db().admin().listDatabases();
      for (const database of databases.databases) {
        const collections = await this.devClient
          .db(database.name)
          .listCollections()
          .toArray();

        counts[database.name] = {
          counts: {},
        };

        for (const collection of collections) {
          const documentCounts = await Promise.all([
            this.devClient
              .db(database.name)
              .collection(collection.name)
              .countDocuments(),
            this.preprodClient
              .db(database.name)
              .collection(collection.name)
              .countDocuments(),
          ]);
          counts[database.name].counts[collection.name] = documentCounts;
        }
      }

      console.log('Counted successfully!');
    } catch (error) {
      console.error(error);
    } finally {
      fs.writeFileSync(
        './out/mongo-counts.json',
        JSON.stringify(counts, null, 2),
      );
    }
  };

  shutdown = () => {
    this.devClient.close();
    this.preprodClient.close();
  };
}
