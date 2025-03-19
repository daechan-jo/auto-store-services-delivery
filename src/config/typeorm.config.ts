import { TypeOrmModuleAsyncOptions } from '@nestjs/typeorm';
import { addTransactionalDataSource } from 'typeorm-transactional';
import { DataSource } from 'typeorm';
import { getDbConfig } from './database.config';

export const TypeormConfig: TypeOrmModuleAsyncOptions = {
  useFactory: () => ({
    ...getDbConfig(),
    autoLoadEntities: true,
    dropSchema: false,
    synchronize: true,
    migrationsRun: true,
    logger: 'advanced-console',
  }),
  async dataSourceFactory(option) {
    if (!option) throw new Error('Invalid options passed');

    if (!global.dataSource) {
      global.dataSource = new DataSource(option);
      await global.dataSource.initialize();
      addTransactionalDataSource(global.dataSource);
    }

    return global.dataSource;
  },
};
