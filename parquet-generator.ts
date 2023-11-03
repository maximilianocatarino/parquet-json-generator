import * as fs from "fs";
import * as path from "node:path";
import { Connection, Database, Statement } from "duckdb-async";
import ParquetSchema, { ParquetTableSchema, ParqueIndexSchema } from "src/infrastructure/persistence/schema/parquet-schema";
import CloundProviderDocument from "src/infrastructure/persistence/schema/cloud-provider";


async function GenerateParquetDatabase(folder: string) {
  const db = await Database.create(":memory:");
  const rows = await db.all("select * from range(1,10)");
  console.log(rows);
}

export default class ParquetDatabaseGenerator {
    private connection: Database;

    private ddl: string[] = [];
    private dml: string[] = [];

    constructor(private schemaFolder: string, private outputDirectory: string) {

    }

    private async getConnection(): Promise<Database> {
        if (!this.connection) {
            this.connection = await Database.create(":memory:");
        }
        return this.connection;
    }

    private async traverseFolder(): Promise<ParquetSchema[]> {
        const collection = [];
        const fileList = await fs.promises.readdir(this.schemaFolder);

        for (const file of fileList) {
            if (file.endsWith('.json')) {
                const content = fs.readFileSync(path.join(this.schemaFolder, file), 'utf-8');
                const schema: ParquetSchema = JSON.parse(content);
                collection.push(schema);
            }
        }
        return Promise.resolve(collection);
    }

    private columnSchemaToColumnStatement(columnListSchema: ParquetTableSchema[]): string {
        let comma = '';
        let statment = '';
        for(const schema of columnListSchema) {
            let key = '';
            if (Object.prototype.hasOwnProperty.call(schema, 'key') && schema.key)
                key = ' PRIMARY KEY';

            let nullable = '';
            if (Object.prototype.hasOwnProperty.call(schema, 'nullable') && schema.nullable)
                nullable = ' NOT NULL';

            let fk = '';
            if (Object.prototype.hasOwnProperty.call(schema, 'foreignKey'))
                fk = ` REFERENCES ${schema.foreignKey.tableName}(${schema.foreignKey.columnName})`;

            statment += `${comma}${schema.columnName} ${schema.type}${key}${nullable}${fk}`;
            comma = ', ';
        }
        return statment;
    }

    private getSchemaByColumnName(columnListSchema: ParquetTableSchema[], name: string): ParquetTableSchema {
        for (const schema of columnListSchema) {
            if (schema.columnName === name)
                return schema;
        }
        return null;
    }

    private columnDataToInsertValue(columnListSchema: ParquetTableSchema[], row: CloundProviderDocument): string {
        let comma = '';
        let values = '';
        let columns = '';
        
        for (const prop in row) {
            columns += `${comma}${prop}`;

            let val = row[prop];
            const schema = this.getSchemaByColumnName(columnListSchema, prop);
            switch (schema.type) {
                case 'varchar':
                    val = `'${row[prop]}'`;
                    break;

                default:
                    val = row[prop];
            }

            values += `${comma}${val}`;
            comma = ',';
        }

        return `(${columns}) values (${values})`;
    }

    private indexSchemaToIndexStatement(tableName: string, schemaList: ParqueIndexSchema[]): string[] {
        const collection = [];

        for (const schema of schemaList) {
            let unique = '';
            if (Object.prototype.hasOwnProperty.call(schema, 'unique') && schema.unique)
                unique = ' UNIQUE'

            let comma = '';
            let fieldList = '';
            for (const field of schema.fields) {
                fieldList += `${comma}${field}`;
                comma = ', ';
            }

            let index = `CREATE INDEX ${schema.name}${unique} ON ${tableName} (${fieldList});`;
            collection.push(index);
        }
        return collection;
    }

    private schemaToCreateStatement(schema: ParquetSchema): void {
        
        const columnSchema = this.columnSchemaToColumnStatement(schema.schema);
        this.ddl.push(`CREATE TABLE ${schema.tableName} (${columnSchema});`);

        const indexList = Object.prototype.hasOwnProperty.call(schema, 'indexes') ? schema.indexes : [];
        const indexes = this.indexSchemaToIndexStatement(schema.tableName, indexList);
        this.dml = this.dml.concat(indexes);

        for (const rowData of schema.rows) {
            const values = this.columnDataToInsertValue(schema.schema, rowData);
            this.dml.push(`INSERT INTO ${schema.tableName} ${values};`);
        }
        const output = path.join(this.outputDirectory, `${schema.tableName}.parquet`);
        this.ddl.push(`copy (select * from ${schema.tableName}) to '${output}' (format 'parquet');`);

        if (Object.prototype.hasOwnProperty.call(schema, 'options')) {
            this.ddl.push(`CREATE SEQUENCE ${schema.tableName}_sequence START 1;`)
        }
    }

    private async runStatementList(statementList: string[]): Promise<boolean> {
        try {
            const db = await this.getConnection();
            for (const sql of statementList) {
                console.log(sql);
                await db.all(sql);
            }
            return true;
        } catch (e) {
            console.log(e);
        }
        return false;
    }

    async create() {
        const schemaList: ParquetSchema[] = await this.traverseFolder();
        for (const schema of schemaList) {
            this.schemaToCreateStatement(schema)
        }

        await this.runStatementList(this.ddl);
        await this.runStatementList(this.dml);
    }
}
