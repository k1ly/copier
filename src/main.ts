import { config } from 'dotenv';
import * as fs from 'fs';
import { TelegramNotification } from './util/telegram-notification';
import { CassandraCopier } from './workers/cassandra-copier';
import { ElasticCopier } from './workers/elastic-copier';
import { GremlinCopier } from './workers/gremlin-copier';
import { MongoCopier } from './workers/mongo-copier';
import { PostgreCopier } from './workers/postgre-copier';

config();

async function main() {
  let cassandraCopier: CassandraCopier;
  let gremlinCopier: GremlinCopier;
  let postgreCopier: PostgreCopier;
  let elasticCopier: ElasticCopier;
  let mongoCopier: MongoCopier;
  let telegramNotification: TelegramNotification;
  try {
    if (!fs.existsSync('./out')) fs.mkdirSync('./out');

    if (process.env.TG_BOT_API_KEY && process.env.TG_BOT_CHAT_ID)
      telegramNotification = new TelegramNotification(
        process.env.TG_BOT_API_KEY,
        process.env.TG_BOT_CHAT_ID,
      );

    cassandraCopier = new CassandraCopier(telegramNotification);
    await cassandraCopier.clearKeyspaces();
    await cassandraCopier.copyKeyspaces();
    await cassandraCopier.getCounts();

    gremlinCopier = new GremlinCopier(telegramNotification);
    await gremlinCopier.clearGraph();
    await gremlinCopier.copyGraph();
    await gremlinCopier.getCounts();

    postgreCopier = new PostgreCopier(telegramNotification);
    await postgreCopier.clearDatabases();
    await postgreCopier.copyDatabases();
    await postgreCopier.getCounts();

    elasticCopier = new ElasticCopier(telegramNotification);
    await elasticCopier.clearIndices();
    await elasticCopier.copyIndices();
    await elasticCopier.getCounts();

    mongoCopier = new MongoCopier(telegramNotification);
    await mongoCopier.clearDatabases();
    await mongoCopier.copyDatabases();
    await mongoCopier.getCounts();
  } finally {
    if (cassandraCopier) cassandraCopier.shutdown();
    if (gremlinCopier) gremlinCopier.shutdown();
    if (postgreCopier) postgreCopier.shutdown();
    if (elasticCopier) elasticCopier.shutdown();
    if (mongoCopier) mongoCopier.shutdown();
  }
}

main();
