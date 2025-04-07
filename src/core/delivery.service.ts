import {
  CoupangInvoice,
  CoupangOrder,
  JobType,
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

  async deliveryManagement(jobId: string) {
    const onchResults: { status: string; data: DeliveryData[] } = await this.rabbitmqService.send(
      'onch-queue',
      'deliveryExtraction',
      {
        jobId: jobId,
        type: JobType.SHIPPING,
        store: this.configService.get<string>('STORE'),
      },
    );

    if (!onchResults || !Array.isArray(onchResults.data) || onchResults.data.length === 0) {
      console.log(`${JobType.SHIPPING}${jobId}: 새로 등록된 운송장이 없습니다.`);
      return;
    }

    const response: { status: string; data: string } = await this.rabbitmqService.send(
      'coupang-queue',
      'newGetCoupangOrderList',
      {
        jobId: jobId,
        type: JobType.SHIPPING,
        data: OrderStatus.INSTRUCT,
      },
    );
    const newOrders: CoupangOrder[] = JSONbig.parse(response.data);

    if (newOrders.length === 0) {
      console.log(`${JobType.SHIPPING}${jobId}: 새로운 출고데이터가 없습니다.`);
      return;
    }

    console.log(`${JobType.SHIPPING}${jobId}: 매치 시작`);

    const rowMap: Record<string, DeliveryData> = {};
    onchResults.data.forEach((row: DeliveryData) => {
      const key = `${row.nameText.replace(/\s/g, '')}-${row.phoneText.replace(/[^0-9]/g, '')}`;
      rowMap[key] = row;
    });

    const matchedOrders = newOrders
      .filter((order) => {
        const key = `${order.receiverName.replace(/\s/g, '')}-${order.receiverMobile.replace(/[^0-9]/g, '')}`;
        return rowMap[key] !== undefined;
      })
      .map((order) => {
        const key = `${order.receiverName.replace(/\s/g, '')}-${order.receiverMobile.replace(/[^0-9]/g, '')}`;
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
          jobId: jobId,
          type: JobType.SHIPPING,
          data: updatedOrders,
        });

      console.log(`${JobType.SHIPPING}${jobId}: 운송장 자동등록 결과`, results.data);

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

          console.log(`${JobType.SHIPPING}${jobId}: 성공 이메일 전송 완료`);
        } catch (error: any) {
          console.error(
            `${JobType.ERROR}${JobType.SHIPPING}${jobId}: 성공 이메일 전송 실패\n`,
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

          console.log(`${JobType.ERROR}${JobType.SHIPPING}${jobId}: 실패 이메일 전송 완료`);
        } catch (error: any) {
          console.error(
            `${JobType.ERROR}${JobType.SHIPPING}${jobId}: 실패 이메일 전송 실패\n`,
            error.response?.data || error.message,
          );
        }
      }
    }
  }

  @Cron('0 */30 * * * *')
  async shippingCron() {
    const jobId = this.utilService.generateCronId();
    try {
      const nowTime = moment().format('HH:mm:ss');
      console.log(`${JobType.SHIPPING}${jobId}-${nowTime}: 운송장 등록 시작`);

      await this.deliveryManagement(jobId);
    } catch (error: any) {
      setImmediate(async () => {
        await this.rabbitmqService.emit('mail-queue', 'sendErrorMail', {
          jobType: JobType.SHIPPING,
          store: this.configService.get<string>('STORE'),
          jobId: jobId,
          message: error.message,
        });
      });

      console.error(`${JobType.ERROR}${JobType.SHIPPING}${jobId}: ${error.message}`);
    } finally {
      console.log(`${JobType.SHIPPING}${jobId}: 운송장 등록 종료`);
    }
  }
}
