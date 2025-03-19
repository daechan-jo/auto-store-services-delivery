import * as dotenv from 'dotenv';
import * as path from 'node:path';
import { DataSourceOptions } from 'typeorm';

dotenv.config({
  path: '/Users/daechanjo/codes/project/auto-store/.env',
});

export const getDbConfig = (): DataSourceOptions => {
  return {
    type: 'postgres',
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT),
    username: process.env.DB_USER,
    password: String(process.env.DB_PASSWORD),
    database: process.env.DB_NAME,
    entities: [path.join(__dirname, '/../entities/*.entity.{js,ts}')],
    migrations: [path.join(__dirname, '/../migrations/**/*.{js,ts}')],
    logging: false,
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
    extra: {
      max: 9,
      keepAlive: true,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
      timezone: 'Asia/Seoul',
    },
  };
};
