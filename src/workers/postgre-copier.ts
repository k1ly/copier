import * as fs from 'fs';
import { Client } from 'pg';
import { TelegramNotification } from '../util/telegram-notification';

export class PostgreCopier {
  private devClients: Client[];
  private preprodClients: Client[];
  private telegramNotification: TelegramNotification;

  constructor(telegramNotification?: TelegramNotification) {
    this.devClients = process.env.PG_DEV_DATABASES.split(',').map(
      (database) =>
        new Client({
          host: process.env.PG_DEV_HOST,
          port: Number(process.env.PG_DEV_PORT),
          user: process.env.PG_DEV_USER,
          password: process.env.PG_DEV_PASSWORD,
          ssl: true,
          database,
        }),
    );
    this.devClients.forEach((client) => client.connect());

    this.preprodClients = process.env.PG_PREPROD_DATABASES.split(',').map(
      (database) =>
        new Client({
          host: process.env.PG_PREPROD_HOST,
          port: Number(process.env.PG_PREPROD_PORT),
          user: process.env.PG_PREPROD_USER,
          password: process.env.PG_PREPROD_PASSWORD,
          ssl: true,
          database,
        }),
    );
    this.preprodClients.forEach((client) => client.connect());

    this.telegramNotification = telegramNotification;
  }

  private copyTypes = async (devDatabase: Client, preprodDatabase: Client) => {
    const typesQuery = `
        SELECT 
          n.nspname AS schema_name,
          t.typname AS type_name,
          e.enumlabel AS enum_value
        FROM pg_type t
          LEFT JOIN pg_enum e ON t.oid = e.enumtypid
          JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE t.typtype = 'e'
          AND t.typname NOT LIKE '\\_%'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY n.nspname, t.typname, e.enumsortorder;
      `;
    const typesResult = await devDatabase.query(typesQuery);
    const types = typesResult.rows;

    const groupedTypes: Record<string, { schema: string; values: string[] }> =
      {};
    types.forEach((row) => {
      if (!groupedTypes[row.type_name]) {
        groupedTypes[row.type_name] = {
          schema: row.schema_name,
          values: [],
        };
      }
      groupedTypes[row.type_name].values.push(row.enum_value);
    });

    for (const [name, { schema, values }] of Object.entries(groupedTypes)) {
      const createTypeQuery = `CREATE TYPE ${schema}.${name} AS ENUM (${values.map((value) => `'${value}'`).join(', ')})`;
      await preprodDatabase.query(createTypeQuery);
    }
  };

  private copySequences = async (
    devDatabase: Client,
    preprodDatabase: Client,
  ) => {
    const sequencesQuery = `
        SELECT
          n.nspname AS schema_name,
          c.relname AS sequence_name,
          pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
          s.seqstart AS start_value,
          s.seqincrement AS increment_by,
          s.seqmax AS max_value,
          s.seqmin AS min_value,
          s.seqcache AS cache_value,
          s.seqcycle AS is_cycled
        FROM pg_sequence s
          JOIN pg_class c ON s.seqrelid = c.oid
          JOIN pg_depend AS d ON c.relfilenode = d.objid
          JOIN pg_attribute AS a ON a.attnum = d.refobjsubid
            AND a.attrelid = d.refobjid
          JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'informatiON_schema')
        ORDER BY n.nspname, c.relname;
      `;
    const sequencesResult = await devDatabase.query(sequencesQuery);
    const sequences = sequencesResult.rows;

    for (const sequence of sequences) {
      const createSequenceQuery = `
          CREATE SEQUENCE "${sequence.schema_name}"."${sequence.sequence_name}"
          AS ${sequence.data_type}
          START WITH ${sequence.start_value}
          INCREMENT BY ${sequence.increment_by}
          MAXVALUE ${sequence.max_value}
          MINVALUE ${sequence.min_value}
          CACHE ${sequence.cache_value}
          ${sequence.is_cycled ? 'CYCLE' : 'NO CYCLE'};
      `;
      await preprodDatabase.query(createSequenceQuery);
    }
  };

  private copyTables = async (devDatabase: Client, preprodDatabase: Client) => {
    const columnsQuery = `
        SELECT 
          n.nspname AS schema_name,
          c.relname AS table_name,
          a.attname AS column_name,
          pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
          CASE 
            WHEN a.attnotnull THEN 'NOT NULL'
            ELSE 'NULL'
          END AS nullable,
          pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_value
        FROM pg_attribute a
          LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid
            AND a.attnum = d.adnum
          JOIN pg_class c ON a.attrelid = c.oid
          JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE a.attnum > 0 
          AND NOT a.attisdropped 
          AND c.relkind = 'r' 
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY n.nspname, c.relname, a.attnum;
      `;
    const columnsResult = await devDatabase.query(columnsQuery);
    const columns = columnsResult.rows;

    const groupedColumns: Record<
      string,
      {
        schema: string;
        columns: {
          columnName: string;
          dataType: string;
          nullable: string;
          defaultValue: string;
        }[];
      }
    > = {};
    columns.forEach((row) => {
      if (!groupedColumns[row.table_name]) {
        groupedColumns[row.table_name] = {
          schema: row.schema_name,
          columns: [],
        };
      }
      groupedColumns[row.table_name].columns.push({
        columnName: row.column_name,
        dataType: row.data_type,
        nullable: row.nullable,
        defaultValue: row.default_value,
      });
    });

    for (const [name, { schema, columns }] of Object.entries(groupedColumns)) {
      const createTableQuery = `
          CREATE TABLE ${schema}.${name} (
            ${columns.map((col) => `${col.columnName} ${col.dataType} ${col.nullable} ${col.defaultValue ? `DEFAULT ${col.defaultValue}` : ''}`).join(', ')}
            );
        `;
      await preprodDatabase.query(createTableQuery);
    }
  };

  private copyData = async (devDatabase: Client, preprodDatabase: Client) => {
    const tablesQuery = `
        SELECT
          n.nspname AS schema_name,
          t.relname AS table_name
        FROM pg_class t
          JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE t.relkind = 'r' 
          AND n.nspname NOT IN ('pg_catalog', 'information_schema');
      `;
    const tablesResult = await devDatabase.query(tablesQuery);
    const tables = tablesResult.rows;

    for (const table of tables) {
      const columnsQuery = `
          SELECT 
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type
          FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
          WHERE a.attnum > 0
            AND NOT a.attisdropped
            AND c.relname = '${table.table_name}'
            AND n.nspname = '${table.schema_name}';
        `;
      const columnsResult = await devDatabase.query(columnsQuery);
      const columns = columnsResult.rows;

      const columnNames = columns
        .map((column) => column.column_name)
        .join(', ');
      const valueClauses = columns.map((column, i) => `$${i + 1}`).join(', ');
      const insertQuery = `
          INSERT INTO ${table.schema_name}.${table.table_name} (${columnNames}) VALUES (${valueClauses});
        `;

      const selectQuery = `
          SELECT * FROM ${table.schema_name}.${table.table_name}
        `;
      const selectResult = await devDatabase.query(selectQuery);
      const rows = selectResult.rows;

      const insertQueriesParams = rows.map((row) =>
        columns.map((column) =>
          column.column_type.startsWith('json')
            ? JSON.stringify(row[column.column_name])
            : row[column.column_name],
        ),
      );
      for (const params of insertQueriesParams) {
        await preprodDatabase.query(insertQuery, params);
      }
    }
  };

  private copyConstraints = async (
    devDatabase: Client,
    preprodDatabase: Client,
  ) => {
    const constraintsQuery = `
        SELECT
          n.nspname AS schema_name,
          cl.relname AS table_name,
          c.conname AS constraint_name,
          pg_catalog.pg_get_constraintdef(c.oid) AS constraint_definition
        FROM pg_constraint c
          JOIN pg_class cl ON c.conrelid = cl.oid
          JOIN pg_namespace n ON c.connamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY n.nspname, cl.relname, c.conname;
      `;
    const constraintsResult = await devDatabase.query(constraintsQuery);
    const constraints = constraintsResult.rows;

    for (const constraint of constraints) {
      const createConstraintQuery = `
          ALTER TABLE ${constraint.schema_name}.${constraint.table_name}
          ADD ${constraint.constraint_definition};
        `;
      await preprodDatabase.query(createConstraintQuery);
    }
  };

  private copyIndexes = async (
    devDatabase: Client,
    preprodDatabase: Client,
  ) => {
    const indexesQuery = `
        SELECT indexdef
        FROM pg_indexes
        WHERE indexdef NOT LIKE '% UNIQUE INDEX %' 
          AND schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schemaname, tablename, indexname;
      `;
    const indexesResult = await devDatabase.query(indexesQuery);
    const indexes = indexesResult.rows;

    for (const index of indexes) {
      const createIndexQuery = index.index_definition;
      await preprodDatabase.query(createIndexQuery);
    }
  };

  private copyFunctions = async (
    devDatabase: Client,
    preprodDatabase: Client,
  ) => {
    const functionsQuery = `
        SELECT pg_catalog.pg_get_functiondef(p.oid) AS function_definition
        FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY n.nspname, p.proname;
      `;
    const functionsResult = await devDatabase.query(functionsQuery);
    const functions = functionsResult.rows;

    for (const func of functions) {
      const createFunctionQuery = func.function_definition;
      await preprodDatabase.query(createFunctionQuery);
    }
  };

  private copyTriggers = async (
    devDatabase: Client,
    preprodDatabase: Client,
  ) => {
    const triggersQuery = `
        SELECT pg_catalog.pg_get_triggerdef(t.oid) AS trigger_definition
        FROM pg_trigger t
          JOIN pg_class c ON t.tgrelid = c.oid
          JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND NOT t.tgisinternal
        ORDER BY n.nspname, c.relname, t.tgname;
      `;
    const triggersResult = await devDatabase.query(triggersQuery);
    const triggers = triggersResult.rows;

    for (const trigger of triggers) {
      const createTriggerQuery = trigger.trigger_definition;
      await preprodDatabase.query(createTriggerQuery);
    }
  };

  copyDatabases = async () => {
    try {
      for (let i = 0; i < this.devClients.length; i++) {
        await this.copyTypes(this.devClients[i], this.preprodClients[i]);
        await this.copySequences(this.devClients[i], this.preprodClients[i]);
        await this.copyTables(this.devClients[i], this.preprodClients[i]);
        await this.copyData(this.devClients[i], this.preprodClients[i]);
        await this.copyIndexes(this.devClients[i], this.preprodClients[i]);
        await this.copyConstraints(this.devClients[i], this.preprodClients[i]);
        await this.copyFunctions(this.devClients[i], this.preprodClients[i]);
        await this.copyTriggers(this.devClients[i], this.preprodClients[i]);
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
      for (const database of this.preprodClients) {
        const triggersQuery = `
        SELECT
          n.nspname AS schema_name,
          c.relname AS table_name,
          t.tgname AS trigger_name
        FROM pg_trigger t
          JOIN pg_class c ON t.tgrelid = c.oid
          JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');
      `;
        const triggersResult = await database.query(triggersQuery);
        const triggers = triggersResult.rows;
        for (const trigger of triggers) {
          await database.query(`
            DROP TRIGGER IF EXISTS ${trigger.trigger_name}
            ON ${trigger.schema_name}.${trigger.table_name};
            `);
        }

        const functionsQuery = `
        SELECT
          n.nspname AS schema_name,
          p.proname AS function_name
        FROM pg_proc p
          JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');
      `;
        const functionsResult = await database.query(functionsQuery);
        const functions = functionsResult.rows;
        for (const func of functions) {
          const { schema_name, function_name } = func;
          await database.query(`
            DROP FUNCTION IF EXISTS ${schema_name}.${function_name} CASCADE;
            `);
        }

        const tablesQuery = `
        SELECT
          n.nspname AS schema_name,
          c.relname AS table_name
        FROM pg_class c
          JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'r' 
          AND n.nspname NOT IN ('pg_catalog', 'information_schema');
      `;
        const tablesResult = await database.query(tablesQuery);
        const tables = tablesResult.rows;
        for (const table of tables) {
          await database.query(`
            DROP TABLE IF EXISTS ${table.schema_name}.${table.table_name} CASCADE;
            `);
        }

        const typesQuery = `
        SELECT
          n.nspname AS schema_name,
          t.typname AS type_name
        FROM pg_type t
          JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND t.typname NOT LIKE '\\_%';
      `;
        const typesResult = await database.query(typesQuery);
        const types = typesResult.rows;
        for (const type of types) {
          await database.query(`
            DROP TYPE IF EXISTS ${type.schema_name}.${type.type_name} CASCADE;
            `);
        }

        const sequencesQuery = `
        SELECT
          n.nspname AS schema_name,
          c.relname AS sequence_name
        FROM pg_sequence s
          JOIN pg_class c ON s.seqrelid = c.oid
          JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');
      `;
        const sequencesResult = await database.query(sequencesQuery);
        const sequences = sequencesResult.rows;
        for (const sequence of sequences) {
          await database.query(`
            DROP SEQUENCE IF EXISTS ${sequence.schema_name}.${sequence.sequence_name} CASCADE;
            `);
        }

        const constraintsQuery = `
        SELECT
          n.nspname AS schema_name,
          cl.relname AS table_name,
          c.conname AS constraint_name
        FROM pg_constraint c
          JOIN pg_class cl ON c.conrelid = cl.oid
          JOIN pg_namespace n ON c.connamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND c.contype = 'f';
      `;
        const constraintsResult = await database.query(constraintsQuery);
        const constraints = constraintsResult.rows;
        for (const constraint of constraints) {
          await database.query(`
            ALTER TABLE ${constraint.schema_name}.${constraint.table_name}
            DROP CONSTRAINT IF EXISTS ${constraint.constraint_name};
            `);
        }

        const indexesQuery = `
        SELECT 
          schemaname AS schema_name,
          indexname AS index_name
        FROM pg_indexes
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
      `;
        const indexesResult = await database.query(indexesQuery);
        const indexes = indexesResult.rows;
        for (const index of indexes) {
          await database.query(`
            DROP INDEX IF EXISTS ${index.schema_name}.${index.index_name};
            `);
        }
      }
      console.log('Cleared successfully!');
    } catch (error) {
      console.error(error);
    }
  };

  getCounts = async () => {
    const counts = {};
    try {
      for (let i = 0; i < this.devClients.length; i++) {
        const typeQuery = `
        SELECT count(*) AS count
        FROM pg_type t
          JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND t.typname NOT LIKE '\\_%';
      `;
        const typeResults = await Promise.all([
          this.devClients[i].query(typeQuery),
          this.preprodClients[i].query(typeQuery),
        ]);
        const typeCounts = typeResults.map((result) => result.rows[0].count);

        const sequenceQuery = `
        SELECT count(*) AS count
        FROM pg_sequence s
          JOIN pg_class c ON s.seqrelid = c.oid
          JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');
      `;
        const sequenceResults = await Promise.all([
          this.devClients[i].query(sequenceQuery),
          this.preprodClients[i].query(sequenceQuery),
        ]);
        const sequenceCounts = sequenceResults.map(
          (result) => result.rows[0].count,
        );

        const indexQuery = `
        SELECT count(*) AS count
        FROM pg_indexes
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
      `;
        const indexResults = await Promise.all([
          this.devClients[i].query(indexQuery),
          this.preprodClients[i].query(indexQuery),
        ]);
        const indexCounts = indexResults.map((result) => result.rows[0].count);

        const constraintQuery = `
        SELECT count(*) AS count
        FROM pg_constraint c
          JOIN pg_namespace n ON c.connamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');
      `;
        const constraintResults = await Promise.all([
          this.devClients[i].query(constraintQuery),
          this.preprodClients[i].query(constraintQuery),
        ]);
        const constraintCounts = constraintResults.map(
          (result) => result.rows[0].count,
        );

        const functionQuery = `
        SELECT count(*) AS count
        FROM pg_proc p
          JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');
      `;
        const functionResults = await Promise.all([
          this.devClients[i].query(functionQuery),
          this.preprodClients[i].query(functionQuery),
        ]);
        const functionCounts = functionResults.map(
          (result) => result.rows[0].count,
        );

        const triggerQuery = `
        SELECT count(*) AS count
        FROM pg_trigger t
          JOIN pg_class c ON t.tgrelid = c.oid
          JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');
      `;
        const triggerResults = await Promise.all([
          this.devClients[i].query(triggerQuery),
          this.preprodClients[i].query(triggerQuery),
        ]);
        const triggerCounts = triggerResults.map(
          (result) => result.rows[0].count,
        );

        counts[
          `${this.devClients[i].database}|${this.preprodClients[i].database}`
        ] = {
          typeCounts,
          sequenceCounts,
          indexCounts,
          constraintCounts,
          functionCounts,
          triggerCounts,
          counts: {},
        };

        const tablesQuery = `
        SELECT
          n.nspname AS schema_name,
          c.relname AS table_name
        FROM pg_class c
          JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'r' 
          AND n.nspname NOT IN ('pg_catalog', 'information_schema');
      `;
        const tablesResult = await this.devClients[i].query(tablesQuery);
        const tables = tablesResult.rows;
        for (const table of tables) {
          const query = `SELECT count(*) FROM ${table.schema_name}.${table.table_name}`;
          const results = await Promise.all([
            this.devClients[i].query(query),
            this.preprodClients[i].query(query),
          ]);
          counts[
            `${this.devClients[i].database}|${this.preprodClients[i].database}`
          ].counts[table.table_name] = results.map(
            (result) => result.rows[0].count,
          );
        }
      }

      console.log('Counted successfully!');
    } catch (error) {
      console.error(error);
    } finally {
      fs.writeFileSync(
        './out/postgre-counts.json',
        JSON.stringify(counts, null, 2),
      );
    }
  };

  shutdown = () => {
    this.devClients.forEach((client) => client.end());
    this.preprodClients.forEach((client) => client.end());
  };
}
