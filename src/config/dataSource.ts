import 'reflect-metadata';
import { DataSource } from 'typeorm';

import { getDbConfig } from './database.config';

const dataSource = new DataSource(getDbConfig());

export const initializeDataSource = async () => {
  if (!dataSource.isInitialized) {
    await dataSource.initialize();
  }
  return dataSource;
};

export default initializeDataSource();
