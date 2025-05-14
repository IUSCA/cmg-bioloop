// require('./project.js');
// require('./convert.js');

const mongoose = require("mongoose");
// var mongoosePaginate = require("mongoose-paginate");
const Schema = mongoose.Schema;

const dataset = new Schema(
  {
    name: {type: String},
    paths: {
      origin: {type: String, default: ""},
      archive: {type: String, default: ""},
      staged: {type: String, default: ""},
    },
    source_node: {type: String},
    size: {type: Number},
    du_size: {type: Number, default: null},
    // takenAt: 2021-09-30T15:17:20.740+00:00
    // createdAt: 2021-10-05T19:39:40.126+00:00
    // updatedAt: 2021-10-05T19:39:40.126+00:00
    description: {type: String, default: null},
    files: {type: Number},
    cbcls: {type: Number},
    checksums: [
      {
        path: {type: String},
        md5: {type: String},
      },
    ],
    directories: {type: Number},
    inspected: {type: Boolean, default: false},
    archived: {type: Boolean, default: false},
    // used to signal to workers to start staging if false
    staged: {type: Boolean, default: false},
    validated: {type: Boolean, default: false},
    converted: {type: Boolean, default: false},
    errored: {type: Schema.Types.Mixed, default: null},
    taken: {type: Schema.Types.ObjectId, ref: "worker", default: null},
    takenAt: {type: Schema.Types.Date, default: Date.now()},
    // a place to track history / when things happen
    events: [
      {
        stamp: {type: Schema.Types.Date, default: Date.now()},
        description: {type: String},
      },
    ],
  },
  {timestamps: true}
);

dataset.index({name: 1}, {unique: true});

// dataset.plugin(mongoosePaginate);

module.exports = mongoose.model("dataset", dataset);
