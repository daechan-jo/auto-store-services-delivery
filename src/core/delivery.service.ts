import {
  CoupangInvoice,
  CoupangOrder,
  CronType,
  DeliveryData,
  InvoiceUploadResult,
  OrderStatus,
} from '@daechanjo/models';
import { RabbitMQService } from '@daechanjo/rabbitmq';
import { UtilService } from '@daechanjo/util';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';
import moment from 'moment-timezone';
import JSONbig from 'json-bigint';

import { courierCode } from '../common/couier';

@Injectable()
export class DeliveryService {
  constructor(
    private readonly utilService: UtilService,
    private readonly configService: ConfigService,
    private readonly rabbitmqService: RabbitMQService,
    @InjectRedis() private readonly redis: Redis,
  ) {}

  async deliveryManagement(cronId: string) {
    const onchResults: { status: string; data: DeliveryData[] } = await this.rabbitmqService.send(
      'onch-queue',
      'deliveryExtraction',
      {
        cronId: cronId,
        store: this.configService.get<string>('STORE'),
        type: CronType.SHIPPING,
      },
    );

    if (!onchResults || !Array.isArray(onchResults.data) || onchResults.data.length === 0) {
      console.log(`${CronType.SHIPPING}${cronId}: 새로 등록된 운송장이 없습니다.`);
      return;
    }

    const response: { status: string; data: string } = await this.rabbitmqService.send(
      'coupang-queue',
      'newGetCoupangOrderList',
      {
        cronId: cronId,
        type: CronType.SHIPPING,
        status: OrderStatus.INSTRUCT,
      },
    );
    const newOrders: CoupangOrder[] = JSONbig.parse(response.data);

    if (newOrders.length === 0) {
      console.log(`${CronType.SHIPPING}${cronId}: 현재 주문이 없습니다.`);
      return;
    }

    console.log(`${CronType.SHIPPING}${cronId}: 매치 시작`);

    const rowMap: Record<string, DeliveryData> = {};
    onchResults.data.forEach((row: DeliveryData) => {
      const key = `${row.nameText}-${row.phoneText}`;
      rowMap[key] = row;
    });

    const matchedOrders = newOrders
      .filter((order) => {
        const key = `${order.receiverName}-${order.receiverMobile}`;
        return rowMap[key] !== undefined;
      })
      .map((order) => {
        const key = `${order.receiverName}-${order.receiverMobile}`;
        return {
          ...order,
          courier: rowMap[key],
        };
      });

    if (matchedOrders.length > 0) {
      const updatedOrders: CoupangInvoice[] = matchedOrders.map((order) => {
        if (order.courier.courier === '경동화물') order.courier.courier = '경동택배';
        const courierName = order.courier.courier;
        const deliveryCompanyCode = courierCode[courierName] || 'DIRECT';

        return {
          ...order,
          deliveryCompanyCode,
          courierName,
        };
      });

      const results: { status: string; data: InvoiceUploadResult[] } =
        await this.rabbitmqService.send('coupang-queue', 'uploadInvoices', {
          cronId: cronId,
          type: CronType.SHIPPING,
          invoices: updatedOrders,
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

  @Cron('0 */30 * * * *')
  async shippingCron() {
    const cronId = this.utilService.generateCronId();
    try {
      const nowTime = moment().format('HH:mm:ss');
      console.log(`${CronType.SHIPPING}${cronId}-${nowTime}: 운송장 등록 시작`);

      await this.deliveryManagement(cronId);
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
