const mongoose = require("mongoose");
// var mongoosePaginate = require("mongoose-paginate");

const Schema = mongoose.Schema;

const dataproduct = new Schema(
  {
    name: {type: String},
    files: [
      //array of the files belonging to a dataproduct conversion group
      {
        path: {type: String},
        size: {type: Number},
        md5: {type: String},
      },
    ],
    paths: {
      archive: {type: String, default: ""},
      staged: {type: String, default: ""},
    },

    file_type: {type: String, default: "fastq"},
    size: {type: Number},
    genome: String,
    genome_type: String,
    // what this product was originally derived from
    dataset: {type: Schema.Types.ObjectId, ref: "dataset"},
    conversion: {type: Schema.Types.ObjectId, ref: "conversion"},
    // distinguish source of data product
    upload: {type: Schema.Types.ObjectId, ref: "upload"},
    groups: [{type: Schema.Types.ObjectId, ref: "Group"}],
    users: [{type: Schema.Types.ObjectId, ref: "User"}],
    // cached status to signal if the data product is currently on disk
    staged: {type: Boolean, default: true},
    visible: {type: Boolean, default: true},
    lastStaged: {type: Schema.Types.Date, default: null},
    // if a product is archived and not already staged
    // requested signals stager to retrieve from SDA
    requested: {type: Boolean, default: false},
    errored: {type: Schema.Types.Mixed, default: null},
    disable_archive: {type: Boolean, default: false},
    taken: {type: Schema.Types.ObjectId, ref: "worker", default: null},
    takenAt: {type: Schema.Types.Date, default: Date.now()},
    // list of email addresses to notify when events happen
    // especially staging completes
    notify: [{type: String}],
    upload_to_s3: {type: Boolean, default: false},
    celery_workflow_id: {type: String},

    // a place to track history / when things happen
    events: [
      {
        stamp: {type: Schema.Types.Date, default: Date.now()},
        description: {type: String},
      },
    ],
    genomeType: {type: String},
    genomeValue: {type: String},
  },
  {timestamps: true}
);

// dataproduct.plugin(mongoosePaginate);

module.exports = mongoose.model("dataproduct", dataproduct);
