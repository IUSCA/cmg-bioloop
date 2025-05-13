const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const worker = new Schema ({
  service: {type: String},
  name: {type: String},
  host: {type: String},
  status: {type: String},
  stdout: {type: String},
  progress: {type: mongoose.Schema.Types.Mixed},
  command: {type: String, default: ''}
}, { timestamps: true });

worker.index({name: 1, host: 1}, {unique: true});

module.exports = mongoose.model('worker', worker);
