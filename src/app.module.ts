import { PlaywrightModule } from '@daechanjo/playwright';
import { RabbitMQModule } from '@daechanjo/rabbitmq';
import { UtilModule } from '@daechanjo/util';
import { Module, OnApplicationBootstrap } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { TypeOrmModule } from '@nestjs/typeorm';
import { RedisModule } from '@nestjs-modules/ioredis';

import { redisConfig } from './config/redis.config';
import { TypeormConfig } from './config/typeorm.config';
import { DeliveryService } from './core/delivery.service';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '/Users/daechanjo/codes/project/auto-store/.env',
    }),
    TypeOrmModule.forRootAsync(TypeormConfig),
    RedisModule.forRootAsync({
      useFactory: () => redisConfig,
    }),
    UtilModule,
    PlaywrightModule,
    RabbitMQModule,
  ],
  controllers: [],
  providers: [DeliveryService],
})
export class AppModule implements OnApplicationBootstrap {
  constructor(private readonly deliveryService: DeliveryService) {}

  async onApplicationBootstrap() {
    setTimeout(async () => {
      // await this.deliveryService.shippingCron();
    });
  }
}
