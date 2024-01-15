import * as Pg from "pg";
export const pool = new Pg.Pool();
import { faker } from "@faker-js/faker";
import crypto from "crypto";

interface ForeignKeyRef {
  constraintName: string;
  referencedSchema: string;
  referencedTable: string;
  referencedColumn: string;
}

interface Column {
  dataType: string;
  isNullable: boolean;
  defaultValue?: string;
  foreignKey?: ForeignKeyRef;
}

interface Table {
  name: string;
  schema: string;
  columns: {
    [key: string]: Column;
  };
}

interface ForeignKeyConstraintTree {
  [schema: string]: {
    [table: string]: {
      [column: string]: ForeignKeyRef;
    };
  };
}

interface UniqueConstraints {
  [schema: string]: {
    [table: string]: Set<string>;
  };
}

async function resolveForeignKeys(
  client: Pg.PoolClient | Pg.ClientBase
): Promise<ForeignKeyConstraintTree> {
  const query = `WITH foreign_keys AS (
  SELECT
    conname,
    conrelid,
    confrelid,
    unnest(conkey)  AS conkey,
    unnest(confkey) AS confkey
  FROM pg_constraint
  WHERE contype = 'f'
)
SELECT
  fk.conname as constraint_name,
  fkns.nspname as schema,
  fkc.relname as table, 
  a.attname as column,
  ns.nspname as foreign_schema,
  c.relname as foreign_table,
  af.attname as referenced_column
FROM foreign_keys fk
JOIN pg_attribute af ON af.attnum = fk.confkey AND af.attrelid = fk.confrelid
JOIN pg_attribute a ON a.attnum = conkey AND a.attrelid = fk.conrelid
JOIN pg_class c ON c.oid = fk.confrelid::regclass
JOIN pg_catalog.pg_namespace as ns ON c.relnamespace = ns.oid
JOIN pg_class fkc ON fkc.oid = fk.conrelid::regclass
JOIN pg_catalog.pg_namespace as fkns ON fkc.relnamespace = fkns.oid
ORDER BY fk.conrelid, fk.conname;`;

  const result = await client.query(query);
  const tree: ForeignKeyConstraintTree = {};
  for (const row of result.rows) {
    const table = row.table;
    const schema = row.schema;
    if (!tree[schema]) {
      tree[schema] = {};
    }
    if (!tree[schema][table]) {
      tree[schema][table] = {};
    }
    const constraint_name = row.constraint_name;
    const referenced_table = row.foreign_table;
    const referenced_schema = row.foreign_schema;
    const referenced_column = row.referenced_column;
    tree[schema][table][row.column] = {
      constraintName: constraint_name,
      referencedSchema: referenced_schema,
      referencedTable: referenced_table,
      referencedColumn: referenced_column,
    };
  }
  return tree;
}

async function resolveUniqueConstraints(
  client: Pg.PoolClient | Pg.ClientBase,
  virtualUniqueConstraints: UniqueConstraints
): Promise<UniqueConstraints> {
  const query = `SELECT c.conname, n.nspname, r.relname, a.attname
    FROM pg_constraint c
  JOIN pg_namespace n ON n.oid = c.connamespace
  JOIN pg_class r ON r.oid = c.conrelid
  JOIN pg_attribute a ON a.attrelid = r.oid AND a.attnum = ANY(c.conkey)
  WHERE c.contype = 'u'`;

  const result = await client.query(query);
  const constraints: UniqueConstraints = structuredClone(
    virtualUniqueConstraints
  );
  for (const row of result.rows) {
    if (!constraints[row.nspname]) {
      constraints[row.nspname] = {};
    }
    if (!constraints[row.nspname][row.relname]) {
      constraints[row.nspname][row.relname] = new Set<string>();
    }
    constraints[row.nspname][row.relname].add(row.attname);
  }
  return constraints;
}

async function resolveTable(
  client: Pg.PoolClient | Pg.ClientBase,
  tableName: string,
  schemaName: string,
  foreignKeyTree: ForeignKeyConstraintTree,
  forceNonNullPercentage: {
    [column: string]: number;
  }
) {
  const columnsQuery = `SELECT column_name, column_default, data_type, is_nullable
  FROM information_schema.columns
 WHERE table_schema = '$1'
   AND table_name   = '$2'
order by ordinal_position`;

  const columns: { [columnName: string]: Column } = Object.fromEntries(
    (await client.query(columnsQuery, [tableName, schemaName])).rows.map(
      (r) => {
        return [
          r.column_name,
          {
            dataType: r.data_type,
            isNullable: r.is_nullable === "YES",
            defaultValue: r.column_default,
            ordinalPosition: r.ordinal_position,
            foreignKey:
              foreignKeyTree[schemaName]?.[tableName]?.[r.column_name],
          },
        ];
      }
    )
  );
  const dependencies: { [schemaName: string]: Set<string> } = {};

  for (const [columnName, column] of Object.entries(columns)) {
    if (
      column.foreignKey &&
      (column.isNullable == false || forceNonNullPercentage[columnName])
    ) {
      const { referencedSchema, referencedTable } = column.foreignKey;
      if (!dependencies[referencedSchema]) {
        dependencies[referencedSchema] = new Set();
      }
      dependencies[referencedSchema].add(referencedTable);
    }
  }

  return {
    columns,
    dependencies,
  };
}

interface TableWithDependencies {
  table: Table;
  dependencies: { [schemaName: string]: Set<string> };
}

interface Visited {
  [schema: string]: Set<string>;
}

function topoSortTables(tables: TableWithDependencies[]) {
  let sorted: Table[] = [];
  let visited: Visited = {};
  let stack: Visited = {};

  function visit(
    table: Table,
    dependencies: { [schemaName: string]: Set<string> }
  ) {
    if (stack[table.schema]?.has(table.name)) {
      throw new Error("Cycle detected in the graph");
    }
    if (visited[table.schema] && visited[table.schema].has(table.name)) {
      return;
    }

    if (!visited[table.schema]) {
      visited[table.schema] = new Set<string>();
    }

    if (!stack[table.schema]) {
      stack[table.schema] = new Set<string>();
    }

    stack[table.schema].add(table.name);
    visited[table.schema].add(table.name);

    for (const depSchema in dependencies) {
      dependencies[depSchema].forEach((depTable) => {
        const dep = tables.find(
          (t) => t.table.name === depTable && t.table.schema === depSchema
        );
        if (dep) {
          visit(dep.table, dep.dependencies);
        }
      });
    }

    stack[table.schema].delete(table.name);
    sorted.push(table);
  }
  tables.forEach((tableWithDeps) =>
    visit(tableWithDeps.table, tableWithDeps.dependencies)
  );
  return sorted;
}

interface Row {
  [foreignKey: string]: unknown;
}

interface TableWithRows {
  table: Table;
  rows: Row[];
}

interface ExtraIntegrityConstraints {
  [schema: string]: {
    [table: string]: {
      // follow the foreign keys
      [column: string]: string[];
    };
  };
}

async function resolveTableTree(
  client: Pg.PoolClient | Pg.ClientBase,
  root: { table: string; schema: string },
  count: number,
  extraIntegrityConstraints: ExtraIntegrityConstraints[],
  virtualUniqueConstraints: UniqueConstraints,
  forceNonNullPercentage: {
    [schema: string]: {
      [table: string]: {
        [column: string]: number;
      };
    };
  }
) {
  const resolvedTables: TableWithDependencies[] = [];
  const unresolvedTables: { table: string; schema: string }[] = [root];
  const uniqueConstraints = await resolveUniqueConstraints(
    client,
    virtualUniqueConstraints
  );
  const foreignKeyTree = await resolveForeignKeys(client);

  for (const { table, schema } of unresolvedTables) {
    const { columns, dependencies } = await resolveTable(
      client,
      table,
      schema,
      foreignKeyTree,
      forceNonNullPercentage[schema]?.[table] || {}
    );
    resolvedTables.push({
      table: {
        name: table,
        schema: schema,
        columns: columns,
      },
      dependencies,
    });
    for (const [schema, tables] of Object.entries(dependencies)) {
      for (const table of tables) {
        // FIXME: Use a better data structure
        if (
          !resolvedTables.find(
            (t) => t.table.name === table && t.table.schema === schema
          )
        ) {
          unresolvedTables.push({ table, schema });
        }
      }
    }
  }
  return topoSortTables(resolvedTables);
}

interface TemporaryTableState {
  [schema: string]: {
    [table: string]: {
      [column: string]: unknown;
    }[]
  }
}

function generateStuff(
  root: Table,
  count: number,
  foreignKeyTree: ForeignKeyConstraintTree,
  uniqueConstraints: UniqueConstraints,
  forceNonNullPercentage: {
    [schema: string]: {
      [table: string]: {
        [column: string]: number;
      };
    };
  }
) {
  const memo: TemporaryTableState = {};
  
  if (!memo[root.schema]) {
    memo[root.schema] = {
    };
  }

  memo[root.schema][root.name] = [];

  for (let i = 0; i < count; i++) {
    const row = {};
    for (const columnName in root.columns) {
    }
  }


  const rootUniqueConstraints = uniqueConstraints[root.schema]?.[root.table];
  if (rootUniqueConstraints) {
    for (const column of rootUniqueConstraints) {
      if (foreignKeyTree[root.schema]?.[root.table]?.[column]) {

      }
    }
  }


}

function generateValueForColumn(column: Column) {
  const { dataType, isNullable } = column;

  if (isNullable && Math.random() < 0.5) {
    return null; // 50% chance to return null for nullable fields
  }

  switch (dataType) {
    case "bigint":
    case "bigserial":
      return faker.number.bigInt();
    case "bit":
    case "bit varying":
      return faker.string.alphanumeric(1); // Simplified bit representation
    case "boolean":
      return faker.datatype.boolean();
    case "box":
    case "circle":
    case "line":
    case "lseg":
    case "path":
    case "point":
    case "polygon":
      return faker.lorem.word(); // Simplified representation for geometric types
    case "bytea":
      return faker.string.alphanumeric(10); // Simplified binary data
    case "character":
    case "character varying":
      return faker.lorem.sentence();
    case "cidr":
    case "inet":
      return faker.internet.ip();
    case "date":
      return faker.date.past().toISOString().split("T")[0];
    case "double precision":
      return faker.number.float();
    case "integer":
      return faker.number.int();
    case "interval":
      return faker.number.int() + " days"; // Simplified interval
    case "json":
    case "jsonb":
      return JSON.stringify({});
    case "macaddr":
    case "macaddr8":
      return faker.internet.mac();
    case "money":
      return faker.finance.amount();
    case "numeric":
      return faker.number.int();
    case "pg_lsn":
      return faker.string.hexadecimal({ length: 10 });
    case "pg_snapshot":
    case "txid_snapshot":
      return faker.lorem.word(); // Simplified representation for snapshot types
    case "real":
      return faker.number.float();
    case "smallint":
      return faker.number.int({ max: 32767 });
    case "smallserial":
    case "serial":
      return faker.number.int({ max: 2147483647 });
    case "text":
      return faker.lorem.text();
    case "time":
    case "time with time zone":
      return faker.date.recent().toISOString().split("T")[1];
    case "timestamp":
    case "timestamp with time zone":
      return faker.date.recent().toISOString();
    case "tsquery":
    case "tsvector":
      return faker.lorem.word();
    case "uuid":
      return crypto.randomUUID();
    case "xml":
      return `<tag>${faker.lorem.sentence()}</tag>`; // Simplified XML
    // Add more cases as needed
    default:
      return "UnsupportedDataType";
  }
}
