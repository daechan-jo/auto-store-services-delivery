import { RedisModuleOptions } from '@nestjs-modules/ioredis';
import * as process from 'node:process';
import * as dotenv from 'dotenv';

dotenv.config({
  path: '/Users/daechanjo/codes/project/auto-store/.env',
});

export const redisConfig: RedisModuleOptions = {
  type: process.env.ENV === 'prod' ? 'cluster' : 'single',
  nodes: [
    {
      host: process.env.REDIS_HOST || '127.0.0.1',
      port: +process.env.REDIS_PORT! || 6379,
    },
  ],
};
