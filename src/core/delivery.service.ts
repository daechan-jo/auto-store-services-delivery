import { CoupangOrderInfo, CronType, DeliveryData, InvoiceUploadResult } from '@daechanjo/models';
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

    // 쿠팡에서 발주정보 조회
    const today = moment().format('YYYY-MM-DD');
    const thirtyDay = moment().subtract(30, 'days').format('YYYY-MM-DD');

    // api로 조회한거랑 웹상에서 네트워크요청 캡쳐한거랑 shipmentBoxId 가 다름.. 뭐야?
    // 909295085462491100
    // [
    // 	{
    // 		"orderId": 25100104633733,
    // 		"shipmentBoxId": 909295085462491137,
    // 		"success": true,
    // 		"errorKey": null,
    // 		"errorMessage": null,
    // 		"splitItems": null
    // 	}
    // ]

    //
    // https://wing.coupang.com/tenants/sfl-portal/delivery/management/dashboard/
    // search?condition=%7B%22nextShipmentBoxId%22%3Anull%2C%22startDate%22%3A%222025-03-01%22%2C%22endDate%22%3A%222025-03-31%22%2C%22deliveryStatus%22%3A%22DEPARTURE%22%2C%22deliveryMethod%22%3Anull%2C%22detailConditionKey%22%3A%22NAME%22%2C%22detailConditionValue%22%3Anull%2C%22selectedComplexConditionKey%22%3Anull%2C%22deliverCode%22%3Anull%2C%22storePickupStatus%22%3Anull%2C%22isEsdToday%22%3Afalse%2C%22isEsdIn5d%22%3Afalse%2C%22countPerPage%22%3A10%2C%22page%22%3A1%2C%22shipmentType%22%3Anull%7D&mockingTestMode=false
    // 이걸 캡쳐하던가
    // https://wing.coupang.com/tenants/sfl-portal/delivery/management/dashboard/
    // search?condition=%7B%22nextShipmentBoxId%22%3Anull%2C%22startDate%22%3A%222025-03-01%22%2C%22endDate%22%3A%222025-03-31%22%2C%22deliveryStatus%22%3A%22INSTRUCT%22%2C%22deliveryMethod%22%3Anull%2C%22detailConditionKey%22%3A%22NAME%22%2C%22detailConditionValue%22%3Anull%2C%22selectedComplexConditionKey%22%3Anull%2C%22deliverCode%22%3Anull%2C%22storePickupStatus%22%3Anull%2C%22isEsdToday%22%3Afalse%2C%22isEsdIn5d%22%3Afalse%2C%22countPerPage%22%3A10%2C%22page%22%3A2%2C%22shipmentType%22%3Anull%7D&mockingTestMode=false

    const coupangOrderList: { status: string; data: CoupangOrderInfo[] } =
      await this.rabbitmqService.send('coupang-queue', 'getCoupangOrderList', {
        cronId: cronId,
        type: CronType.SHIPPING,
        // INSTRUCT | DEPARTURE
        status: 'DEPARTURE',
        vendorId: this.configService.get<string>('COUPANG_VENDOR_ID'),
        today: today,
        yesterday: thirtyDay,
      });

    if (coupangOrderList.data.length === 0) {
      console.log(`${CronType.SHIPPING}${cronId}: 현재 주문이 없습니다.`);
      return;
    }

    console.log(`${CronType.SHIPPING}${cronId}: 매치 시작`);
    const rowMap: Record<string, DeliveryData> = {};
    onchResults.data.forEach((row: DeliveryData) => {
      const key = `${row.nameText}-${row.phoneText}`;
      rowMap[key] = row;
    });

    const matchedOrders = coupangOrderList.data
      .filter((order: CoupangOrderInfo) => {
        const key = `${order.receiver.name}-${order.receiver.safeNumber}`;
        return rowMap[key] !== undefined;
      })
      .map((order: CoupangOrderInfo) => {
        const key = `${order.receiver.name}-${order.receiver.safeNumber}`;
        return {
          ...order,
          courier: rowMap[key],
        };
      });

    if (matchedOrders.length > 0) {
      const updatedOrders = matchedOrders.map((order) => {
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
