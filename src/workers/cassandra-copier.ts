import { Client, auth } from 'cassandra-driver';
import * as fs from 'fs';
import { TelegramNotification } from '../util/telegram-notification';

export class CassandraCopier {
  private devClient: Client;
  private preprodClient: Client;
  private telegramNotification: TelegramNotification;

  constructor(telegramNotification?: TelegramNotification) {
    this.devClient = new Client({
      contactPoints: [process.env.CASSANDRA_DEV_HOST],
      protocolOptions: {
        port: Number(process.env.CASSANDRA_DEV_PORT),
      },
      localDataCenter: process.env.CASSANDRA_DEV_DATACENTER,
      authProvider: new auth.PlainTextAuthProvider(
        process.env.CASSANDRA_DEV_USERNAME,
        process.env.CASSANDRA_DEV_PASSWORD,
      ),
      sslOptions: {
        rejectUnauthorized: process.env.SSL_VALIDATE === 'true',
        timeout: 300000,
      },
    });

    this.preprodClient = new Client({
      contactPoints: [process.env.CASSANDRA_PREPROD_HOST],
      protocolOptions: {
        port: Number(process.env.CASSANDRA_DEV_PORT),
      },
      localDataCenter: process.env.CASSANDRA_DEV_DATACENTER,
      authProvider: new auth.PlainTextAuthProvider(
        process.env.CASSANDRA_PREPROD_USERNAME,
        process.env.CASSANDRA_PREPROD_PASSWORD,
      ),
      sslOptions: { rejectUnauthorized: process.env.SSL_VALIDATE === 'true' },
    });

    this.telegramNotification = telegramNotification;
  }

  private copyTypes = async (keyspaceName: string) => {
    const typesQuery = `SELECT * FROM system_schema.types WHERE keyspace_name = '${keyspaceName}'`;
    const typesResult = await this.devClient.execute(typesQuery);
    const types = typesResult.rows;
    for (const type of types) {
      const fields = type.field_names
        .map((field, i) => {
          return `${field} ${type.field_types[i]}`;
        })
        .join(', ');
      const createTypeQuery = `CREATE TYPE ${keyspaceName}.${type.type_name} (${fields})`;
      await this.preprodClient.execute(createTypeQuery);
    }
  };

  private copyTables = async (keyspaceName: string) => {
    const tablesQuery = `SELECT table_name FROM system_schema.tables WHERE keyspace_name = '${keyspaceName}'`;
    const tablesResult = await this.devClient.execute(tablesQuery);
    const tables = tablesResult.rows;
    for (const table of tables) {
      const tableName = table.table_name;

      const columnsQuery = `SELECT * FROM system_schema.columns WHERE keyspace_name = '${keyspaceName}' AND table_name = '${tableName}'`;
      const result = await this.devClient.execute(columnsQuery);
      const columns = result.rows;

      const partitionKeys = columns
        .filter((column) => column.kind === 'partition_key')
        .map((column) => column.column_name);
      const clusteringKeys = columns
        .filter((column) => column.kind === 'clustering')
        .map((column) => column.column_name);

      const clusteringOrder = columns
        .filter(
          (column) =>
            column.kind === 'clustering' && column.clustering_order !== 'none',
        )
        .map((column) => `${column.column_name} ${column.clustering_order}`)
        .join(', ');

      const columnNamesAndTypes = columns.map(
        (column) => `${column.column_name} ${column.type}`,
      );
      const createTableQuery = `CREATE TABLE ${keyspaceName}.${tableName} (
      ${columnNamesAndTypes.join(', ')}, 
      PRIMARY KEY (${
        partitionKeys.length > 1
          ? `(${partitionKeys.join(', ')})`
          : partitionKeys[0]
      }${clusteringKeys.length > 0 ? `, ${clusteringKeys.join(', ')}` : ''})
    )${clusteringKeys.length > 0 ? ` WITH CLUSTERING ORDER BY (${clusteringOrder})` : ''}`;

      await this.preprodClient.execute(createTableQuery);
    }
  };

  private copyData = async (keyspaceName: string) => {
    const tablesQuery = `SELECT table_name FROM system_schema.tables WHERE keyspace_name = '${keyspaceName}'`;
    const tablesResult = await this.devClient.execute(tablesQuery);
    const tables = tablesResult.rows;
    for (const table of tables) {
      const tableName = table.table_name;
      const columnsQuery = `SELECT * FROM system_schema.columns WHERE keyspace_name = '${keyspaceName}' AND table_name = '${tableName}'`;
      const columnsResult = await this.devClient.execute(columnsQuery);
      const columns = columnsResult.rows;
      const counterColumns = columns
        .filter((column) => column.type.includes('counter'))
        .map((column) => column.column_name);
      const keys = [
        ...columns
          .filter((column) => column.kind === 'partition_key')
          .map((column) => column.column_name),
        ...columns
          .filter((column) => column.kind === 'clustering')
          .map((column) => column.column_name),
      ];
      const nonCounterColumns = columns
        .filter(
          (column) =>
            !column.type.includes('counter') &&
            !keys.includes(column.column_name),
        )
        .map((column) => column.column_name);

      const selectQuery = `SELECT * FROM ${keyspaceName}.${tableName}`;
      const rows = await new Promise<any[]>((resolve, reject) => {
        let rows = [];
        this.devClient
          .stream(selectQuery, [], { autoPage: true, fetchSize: 100 })
          .on('readable', function () {
            let row;
            while ((row = this.read())) {
              rows.push(row);
            }
          })
          .on('end', () => resolve(rows))
          .on('error', (err) => reject(err));
      });

      if (counterColumns.length > 0) {
        const setClauses = counterColumns
          .map((column) => `${column} = ${column} + ?`)
          .join(', ');
        const whereClauses = keys.map((key) => `${key} = ?`).join(' AND ');
        const updateQuery = `UPDATE ${table} SET ${setClauses} WHERE ${whereClauses}`;
        const updateQueriesParams = rows.map((row) => [
          ...counterColumns.map((column) =>
            row[column] !== null ? row[column] : 0,
          ),
          ...keys.map((key) => row[key]),
        ]);
        for (const params of updateQueriesParams) {
          await this.preprodClient.execute(updateQuery, params, {
            prepare: true,
            logged: false,
          });
        }

        if (nonCounterColumns.length > 0) {
          const setClauses = nonCounterColumns
            .map((column) => `${column} = ?`)
            .join(', ');
          const whereClauses = keys.map((key) => `${key} = ?`).join(' AND ');
          const updateQuery = `UPDATE ${table} SET ${setClauses} WHERE ${whereClauses}`;
          const updateQueriesParams = rows.map((row) => [
            ...nonCounterColumns.map((column) => row[column]),
            ...keys.map((key) => row[key]),
          ]);
          for (const params of updateQueriesParams) {
            await this.preprodClient.execute(updateQuery, params, {
              prepare: true,
              logged: false,
            });
          }
        }
      } else {
        const columnNames = columns
          .map((column) => column.column_name)
          .join(', ');
        const valueClauses = columns.map((column) => '?').join(', ');
        const insertQuery = `INSERT INTO ${table} (${columnNames}) VALUES (${valueClauses})`;
        const insertQueries = rows.map((row) => ({
          query: insertQuery,
          params: columns.map((column) => row[column.column_name]),
        }));
        for (let i = 0; i < insertQueries.length; i += 100) {
          const queriesBatch = insertQueries.slice(i, i + 100);
          await this.preprodClient.batch(queriesBatch, {
            prepare: true,
            logged: false,
          });
        }
      }
    }
  };

  copyKeyspaces = async () => {
    try {
      const keyspacesQuery =
        'SELECT keyspace_name FROM system_schema.keyspaces';
      const keyspacesResult = await this.devClient.execute(keyspacesQuery);
      const keyspaces = keyspacesResult.rows.filter(
        (keyspace) =>
          ![
            'system',
            'system_schema',
            'system_auth',
            'system_distributed',
            'system_traces',
          ].includes(keyspace.keyspace_name),
      );
      for (const keyspace of keyspaces) {
        const keyspaceName = keyspace.keyspace_name;
        const createKeyspaceQuery = `CREATE KEYSPACE ${keyspaceName} WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`;
        await this.preprodClient.execute(createKeyspaceQuery);

        await this.copyTypes(keyspaceName);
        await this.copyTables(keyspaceName);
        await this.copyData(keyspaceName);
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

  clearKeyspaces = async () => {
    try {
      const keyspacesQuery =
        'SELECT keyspace_name FROM system_schema.keyspaces';
      const keyspacesResult = await this.preprodClient.execute(keyspacesQuery);
      const keyspaces = keyspacesResult.rows.filter(
        (keyspace) =>
          ![
            'system',
            'system_schema',
            'system_auth',
            'system_distributed',
            'system_traces',
          ].includes(keyspace.keyspace_name),
      );
      for (const keyspace of keyspaces) {
        const keyspaceName = keyspace.keyspace_name;
        const dropKeyspaceQuery = `DROP KEYSPACE ${keyspaceName};`;
        await this.preprodClient.execute(dropKeyspaceQuery);
      }
      console.log('Cleared successfully!');
    } catch (error) {
      console.error(error);
    }
  };

  getCounts = async () => {
    const counts = {};
    try {
      const keyspacesQuery =
        'SELECT keyspace_name FROM system_schema.keyspaces';
      const keyspacesResult = await this.devClient.execute(keyspacesQuery);
      const keyspaces = keyspacesResult.rows
        .map((keyspace) => keyspace.keyspace_name)
        .filter(
          (keyspaceName) =>
            ![
              'system',
              'system_schema',
              'system_auth',
              'system_distributed',
              'system_traces',
            ].includes(keyspaceName),
        );
      for (const keyspaceName of keyspaces) {
        const typeQuery = `SELECT * FROM system_schema.types WHERE keyspace_name = '${keyspaceName}'`;
        const typeResults = await Promise.all([
          this.devClient.execute(typeQuery),
          this.preprodClient.execute(typeQuery),
        ]);
        const typeCounts = typeResults.map((result) => result.rows.length);

        counts[keyspaceName] = {
          typeCounts,
          counts: {},
        };

        const tablesQuery = `SELECT table_name FROM system_schema.tables WHERE keyspace_name = '${keyspaceName}'`;
        const tablesResult = await this.devClient.execute(tablesQuery);
        const tables = tablesResult.rows.map((table) => table.table_name);
        for (const tableName of tables) {
          const query = `SELECT count(*) FROM ${keyspaceName}.${tableName}`;
          const results = await Promise.all([
            this.devClient.execute(query),
            this.preprodClient.execute(query),
          ]);
          counts[keyspaceName].counts[tableName] = results.map(
            (result) => result.first()['system.count(*)'].low,
          );
        }
      }

      console.log('Counted successfully!');
    } catch (error) {
      console.error(error);
    } finally {
      fs.writeFileSync(
        './out/cassandra-counts.json',
        JSON.stringify(counts, null, 2),
      );
    }
  };

  shutdown = () => {
    this.preprodClient.shutdown();
    this.devClient.shutdown();
  };
}
