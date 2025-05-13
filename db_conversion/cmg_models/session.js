const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const session = new Schema ({
  user: { type: Schema.Types.ObjectId, ref: 'User' },
  title: String,
  genome: String,
  genome_type: String,
  tracks: [
    {
      filename: String,
      dataproduct: { type: Schema.Types.ObjectId, ref: 'dataproduct' },
      color: String,
      title: String,
      size: Number
    }
  ],
  internal_share: [
    { type: Schema.Types.ObjectId, ref: 'User' },
  ],
  external_share: [{type: String}],
  access_count: { type: Number, default: 0},
  staging: {
    requested: Object,
    completed: Boolean,
    notify: String,
    requestedAt: Date
  }

}, { timestamps: true });

module.exports = mongoose.model('session', session);
