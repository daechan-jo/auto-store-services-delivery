import { DataSource } from 'typeorm';

import { getDbConfig } from './database.config';

export const dataSourceCli = new DataSource({
  ...getDbConfig(),
  synchronize: true,
});
