'use strict';

const ExternalTaskSampleWorker = require('./dist/commonjs/index').ExternalTaskSampleWorker;

function registerInContainer(container) {

  container.register('ExternalTaskSampleWorker', ExternalTaskSampleWorker)
    .dependencies('ExternalTaskApiClientService')
    .configure('external_task:sample_worker')
    .singleton();
}

module.exports.registerInContainer = registerInContainer;
