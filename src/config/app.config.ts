export class AppConfig {
  private static instance: AppConfig;
  private _appName: string = 'unknown-service';

  private constructor() {}

  static getInstance(): AppConfig {
    if (!AppConfig.instance) {
      AppConfig.instance = new AppConfig();
    }
    return AppConfig.instance;
  }

  get appName(): string {
    return this._appName;
  }

  set appName(name: string) {
    this._appName = name;
  }
}
