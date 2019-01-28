import * as bluebird from 'bluebird';
import {Logger} from 'loggerhythm';

import {IIdentity} from '@essential-projects/iam_contracts';

import {ExternalTask, IExternalTaskApi} from '@process-engine/external_task_api_contracts';

const logger: Logger = Logger.createLogger('processengine:external_task:sample_worker');

/**
 * Contains a sample implementation for an ExternalTask worker.
 * Can be used for integration tests.
 */
export class ExternalTaskSampleWorker {

  public config: any;

  private _externalTaskApiClient: IExternalTaskApi;

  private _intervalTimer: any;

  private _sampleIdentity: IIdentity = {
    token: 'ZHVtbXlfdG9rZW4=',
    userId: 'defaultUser',
  };

  constructor(externalTaskApiClient: IExternalTaskApi) {
    this._externalTaskApiClient = externalTaskApiClient;
  }

  public start<TPayload, TResult>(): void {
    this._intervalTimer = setInterval(async() => {
      await this._fetchAndProcessExternalTasks<TPayload, TResult>();
    }, this.config.pollingInterval);
  }

  public stop(): void {
    clearInterval(this._intervalTimer);
  }

  /**
   * Callback function for the worker timeout-interval.
   * Checks if some ExternalTasks are available for processing.
   * If so, the global timer interval is interrupted, until all
   * available ExternalTasks have been processed.
   *
   * @async
   */
  private async _fetchAndProcessExternalTasks<TPayload, TResult>(): Promise<void> {

    const availableExternalTasks: Array<ExternalTask<TPayload>> =
      await this
        ._externalTaskApiClient
        .fetchAndLockExternalTasks<TPayload>(this._sampleIdentity,
                                                 this.config.workerId,
                                                 this.config.topicName,
                                                 this.config.maxTasks,
                                                 this.config.longPollingTimeout,
                                                 this.config.lockDuration);

    if (availableExternalTasks.length > 0) {
      logger.info(`Found ${availableExternalTasks.length} ExternalTasks available for processing.`);

      this.stop();

      await bluebird.each(availableExternalTasks, async(externalTask: ExternalTask<TPayload>) => {
        return this._processExternalTask<TPayload, TResult>(externalTask);
      });

      logger.info('All tasks processed.');
      this.start();
    }
  }

  /**
   * Processes the given ExternalTask.
   *
   * @async
   */
  private async _processExternalTask<TPayload, TResult>(externalTask: ExternalTask<TPayload>): Promise<void> {

    logger.info(`Processing ExternalTask ${externalTask.id}.`);

    const result: TResult = await this._getSampleResult<TPayload>(externalTask.payload);

    await this._externalTaskApiClient.finishExternalTask<TResult>(this._sampleIdentity, this.config.workerId, externalTask.id, result);

    logger.info(`Finished processing ExternalTask with ID ${externalTask.id}.`);

  }

  /**
   * Returns some sample result with which to finish the ExternalTasks.
   *
   * @returns The sample result.
   */
  private _getSampleResult<TPayload>(payload: TPayload): any {

    const sampleResult: any = {
      testResults: payload,
    };

    return sampleResult;
  }

}
