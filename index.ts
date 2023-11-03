import os from 'os';
import ParquetDatabaseGenerator from './parquet-generator';
import path from 'node:path';

const root = path.resolve(`${__dirname}${path.sep}..`);
const currentDirectory = __dirname;
const schemaDir = path.join(root, 'schema');
const outputDir = path.join(root, 'parquet');

const generator = new ParquetDatabaseGenerator(schemaDir, outputDir);
generator.create();
