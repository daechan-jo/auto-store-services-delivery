import { CronType } from '@daechanjo/models';
import { RabbitMQService } from '@daechanjo/rabbitmq';
import { UtilService } from '@daechanjo/util';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';
import moment from 'moment-timezone';

import { courierCode } from '../common/couier';

@Injectable()
export class DeliveryService {
  constructor(
    private readonly utilService: UtilService,
    private readonly configService: ConfigService,
    private readonly rabbitmqService: RabbitMQService,
    @InjectRedis() private readonly redis: Redis,
  ) {}

  async waybillManagement(cronId: string) {
    // const lastCronTimeString = await this.redis.get('lastRun:shipping');
    // let lastCronTime: Date;
    //
    // if (lastCronTimeString) {
    //   lastCronTime = this.utilService.convertKoreaTime(lastCronTimeString);
    //
    //   console.log(`${CronType.SHIPPING}${cronId}: 마지막 실행 시간 ${lastCronTime}`);
    // } else {
    //   lastCronTime = this.utilService.createYesterdayKoreaTime();
    //
    //   console.log(
    //     `${CronType.SHIPPING}${cronId}: 마지막 실행시간이 없습니다. 24시간 전으로 설정합니다.`,
    //   );
    // }
    //
    // await this.redis.set(
    //   'lastRun:shipping',
    //   moment.tz('Asia/Seoul').subtract(6, 'hours').toISOString(),
    // );

    const lastCronTime = this.utilService.createYesterdayKoreaTime();

    const onchResults = await this.rabbitmqService.send('onch-queue', 'waybillExtraction', {
      cronId: cronId,
      store: this.configService.get<string>('STORE'),
      lastCronTime: lastCronTime,
      type: CronType.SHIPPING,
    });

    if (!onchResults || !Array.isArray(onchResults.data) || onchResults.data.length === 0) {
      console.log(`${CronType.SHIPPING}${cronId}: 새로 등록된 운송장이 없습니다.`);
      return;
    }

    // 쿠팡에서 발주정보 조회
    const today = moment().format('YYYY-MM-DD');
    const thirtyDay = moment().subtract(30, 'days').format('YYYY-MM-DD');

    console.log(`${CronType.SHIPPING}${cronId}: 쿠팡서비스에 "getCoupangOrderList" 메시지 송신`);
    const coupangOrderList = await this.rabbitmqService.send(
      'coupang-queue',
      'getCoupangOrderList',
      {
        cronId: cronId,
        type: CronType.SHIPPING,
        status: 'INSTRUCT',
        vendorId: this.configService.get<string>('L_COUPANG_VENDOR_ID'),
        today: today,
        yesterday: thirtyDay,
      },
    );

    console.log(`${CronType.SHIPPING}${cronId}: 쿠팡서비스에 "getCoupangOrderList" 메시지 수신`);
    console.log(`${CronType.SHIPPING}${cronId}: 매치 시작`);

    const rowMap = {};
    onchResults.data.forEach((row: any) => {
      const key = `${row.name}-${row.phone}`;
      rowMap[key] = row;
    });

    const matchedOrders = coupangOrderList.data
      .filter((order: any) => {
        const key = `${order.receiver.name}-${order.receiver.safeNumber}`;
        return rowMap[key] !== undefined;
      })
      .map((order: any) => {
        const key = `${order.receiver.name}-${order.receiver.safeNumber}`;
        return {
          ...order,
          courier: rowMap[key],
        };
      });

    if (matchedOrders.length > 0) {
      const updatedOrders = matchedOrders.map((order: any) => {
        if (order.courier.courier === '경동화물') order.courier.courier = '경동택배';
        const courierName = order.courier.courier;
        const deliveryCompanyCode = courierCode[courierName] || 'DIRECT';

        return {
          ...order,
          deliveryCompanyCode,
          courierName,
        };
      });

      const results = await this.rabbitmqService.send('coupang-queue', 'invoiceUpload', {
        cronId: cronId,
        updatedOrders: updatedOrders,
        type: CronType.SHIPPING,
      });

      console.log(`${CronType.SHIPPING}${cronId}: 운송장 자동등록 결과`, results.data);

      const successInvoiceUploads = results.data.filter(
        (result: any) => result.status === 'success',
      );
      const failedInvoiceUploads = results.data.filter((result: any) => result.status === 'failed');

      if (successInvoiceUploads.length > 0) {
        try {
          setImmediate(async () => {
            await this.rabbitmqService.emit('mail-queue', `sendSuccessInvoiceUpload`, {
              successInvoiceUploads: successInvoiceUploads,
              store: this.configService.get<string>('STORE'),
            });
          });

          console.log(`${CronType.SHIPPING}${cronId}: 성공 이메일 전송 완료`);
        } catch (error: any) {
          console.error(
            `${CronType.ERROR}${CronType.SHIPPING}${cronId}: 성공 이메일 전송 실패\n`,
            error.message,
          );
        }
      }

      if (failedInvoiceUploads.length > 0) {
        try {
          setImmediate(async () => {
            await this.rabbitmqService.emit('mail-queue', `sendFailedInvoiceUpload`, {
              successInvoiceUploads: failedInvoiceUploads,
              store: this.configService.get<string>('STORE'),
            });
          });

          console.log(`${CronType.ERROR}${CronType.SHIPPING}${cronId}: 실패 이메일 전송 완료`);
        } catch (error: any) {
          console.error(
            `${CronType.ERROR}${CronType.SHIPPING}${cronId}: 실패 이메일 전송 실패\n`,
            error.response?.data || error.message,
          );
        }
      }
    }
  }

  @Cron('0 */10 * * * *')
  async shippingCron() {
    const cronId = this.utilService.generateCronId();
    try {
      const nowTime = moment().format('HH:mm:ss');
      console.log(`${CronType.SHIPPING}${cronId}-${nowTime}: 운송장 등록 시작`);

      await this.waybillManagement(cronId);
    } catch (error: any) {
      setImmediate(async () => {
        await this.rabbitmqService.emit('mail-queue', 'sendErrorMail', {
          cronType: CronType.SHIPPING,
          store: this.configService.get<string>('STORE'),
          cronId: cronId,
          message: error.message,
        });
      });

      console.error(`${CronType.ERROR}${CronType.SHIPPING}${cronId}: ${error.message}`);
    } finally {
      console.log(`${CronType.SHIPPING}${cronId}: 운송장 등록 종료`);
    }
  }
}
