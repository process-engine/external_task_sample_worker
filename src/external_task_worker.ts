import * as bluebird from 'bluebird';
import {Logger} from 'loggerhythm';

import {HttpClient} from '@essential-projects/http';
import {IIdentity} from '@essential-projects/iam_contracts';

import {ExternalTask, IExternalTaskApi} from '@process-engine/external_task_api_contracts';
import {Model} from '@process-engine/process_engine_contracts';

const logger: Logger = Logger.createLogger('processengine:external_task:sample_worker');

/**
 * Contains a sample implementation for an ExternalTask worker.
 * Can be used for integration tests.
 */
export class ExternalTaskSampleWorker {

  public config: any;

  private _externalTaskApiClient: IExternalTaskApi;
  private _httpClient: HttpClient;

  private _intervalTimer: any;

  private sampleIdentity: IIdentity = {
    token: 'defaultUser',
  };

  constructor(externalTaskApiClient: IExternalTaskApi) {
    this._externalTaskApiClient = externalTaskApiClient;
  }

  public initialize(): void {
    this._httpClient = new HttpClient();
  }

  public start<TPayloadType, TResultType>(): void {
    this._intervalTimer = setInterval(async() => {
      await this._fetchAndProcessExternalTasks<TPayloadType, TResultType>();
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
  private async _fetchAndProcessExternalTasks<TPayloadType, TResultType>(): Promise<void> {

    const availableExternalTasks: Array<ExternalTask<TPayloadType>> =
      await this
        ._externalTaskApiClient
        .fetchAndLockExternalTasks<TPayloadType>(this.sampleIdentity,
                                                 this.config.workerId,
                                                 this.config.topicName,
                                                 this.config.maxTasks,
                                                 this.config.longPollingTimeout,
                                                 this.config.lockDuration);

    if (availableExternalTasks.length > 0) {
      logger.info(`Found ${availableExternalTasks.length} ExternalTasks available for processing.`);

      this.stop();

      await bluebird.each(availableExternalTasks, async(externalTask: ExternalTask<TPayloadType>) => {
        return this._processExternalTask<TPayloadType, TResultType>(externalTask);
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
  private async _processExternalTask<TPayloadType, TResultType>(externalTask: ExternalTask<TPayloadType>): Promise<void> {

    logger.info(`Processing ExternalTask ${externalTask.id}.`);
    const externalTaskInvocation: Model.Activities.ExternalTaskInvocation = this._parseInvocation(externalTask.payload);

    if (externalTaskInvocation) {
      try {
        logger.info('Invocation attached to ExternalTask: ', externalTaskInvocation);

        const result: TResultType = await this._executeInvocation<TResultType>(externalTaskInvocation);

        await this._externalTaskApiClient.finishExternalTask<TResultType>(this.sampleIdentity, this.config.workerId, externalTask.id, result);

        logger.info(`Finished processing ExternalTask with ID ${externalTask.id}.`);
      } catch (error) {
        const message: string = 'Failed to execute ExternalTask!';
        logger.error(message, error);

        await this
          ._externalTaskApiClient
          .handleServiceError(this.sampleIdentity, this.config.workerId, externalTask.id, message, JSON.stringify(error));
      }
    } else {
      const notSupportedError: string = `Invalid job configuration for ExternalTask with ID ${externalTask.id}`;
      logger.error(notSupportedError, externalTaskInvocation);

      await this
        ._externalTaskApiClient
        .handleServiceError(this.sampleIdentity, this.config.workerId, externalTask.id, notSupportedError, JSON.stringify(externalTask.payload));
    }
  }

  /**
   * Tries to parse the given data object into an ExternalTaskInvocation.
   * If some required properties are missing, an empty object is returned.
   *
   * @param   data The data object from which to create an invocation.
   * @returns      The parsed invocation.
   */
  private _parseInvocation(data: any): Model.Activities.ExternalTaskInvocation {

    const urlProperty: string = data['url'];
    const methodProperty: string = data['method'];
    const headersProperty: string = data['headers'];
    const payloadProperty: string = data['payload'];

    if (!(urlProperty && methodProperty)) {
      return undefined;
    }

    const externalTaskInvocation: Model.Activities.ExternalTaskInvocation = new Model.Activities.ExternalTaskInvocation();

    externalTaskInvocation.url = urlProperty;
    externalTaskInvocation.method = methodProperty;
    externalTaskInvocation.headers = headersProperty ? headersProperty : '{}';
    externalTaskInvocation.payload = payloadProperty ? payloadProperty : '{}';

    return externalTaskInvocation;
  }

  /**
   * Uses the HTTP client to execute the given ExternalTaskInvocation.
   *
   * @async
   * @param   invocation The invocation to execute.
   * @returns            The result of the HTTP request.
   */
  private async _executeInvocation<TResultType>(invocation: Model.Activities.ExternalTaskInvocation): Promise<TResultType> {

    const method: string = invocation.method.toLowerCase();

    const options: any = {};

    if (invocation.headers) {
      options.headers = JSON.parse(invocation.headers);
    }

    let result: any;

    logger.info('Calling: ', invocation.url);
    if (method === 'get') {
      result = await this._httpClient.get<TResultType>(invocation.url, options);
    } else {
      result = await this._httpClient[method](invocation.url, invocation.payload, options);
    }

    logger.info('Received the following response: ', result);

    return <TResultType> result;
  }

}
