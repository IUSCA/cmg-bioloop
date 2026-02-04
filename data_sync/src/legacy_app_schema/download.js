const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const download = new Schema ({
  status: {type: String, default: 'new'},
  user:{ type: Schema.Types.ObjectId, ref: 'User' },
  uuid: {type: String, default: ''},
  files: { type: Schema.Types.ObjectId, ref: 'dataproduct' },
  sharing: [
      { type: mongoose.Schema.Types.Mixed }
    ],
  worker: { type: Schema.Types.ObjectId, ref: 'worker' },

}, { timestamps: true });

module.exports = mongoose.model('download', download);
