const mongoose = require("mongoose");
var mongoosePaginate = require("mongoose-paginate");

const Schema = mongoose.Schema;

const upload = new Schema(
	{
		user: { type: Schema.Types.ObjectId, ref: "User" },
		dataset: { type: Schema.Types.ObjectId, ref: "dataset" },
		// refers to the source used to derive this upload
		// e.g. fastq used to generate bam
		// not to be confused with the dataproduct that an upload becomes
		dataproduct: { type: Schema.Types.ObjectId, ref: "dataproduct" },
		path: { type: String },
		notes: { type: String },
		worker: { type: Schema.Types.ObjectId, ref: "worker" },
		status: { type: String },
		file_type: { type: String, default: "fastq" },
		selected: { type: Array },
		file_count: { type: Number },
		projects: [{ type: Schema.Types.ObjectId, ref: "project" }],
		new_project: { type: Boolean, default: true },
    genomeType: { type: String },
    genomeValue: { type: String },

		// Functionality replaced by projects schema association (2021.10)
		// remove this from upload schema ?
		groups: [{ type: Schema.Types.ObjectId, ref: "Group" }],
	},
	{ timestamps: true }
);

upload.plugin(mongoosePaginate);

module.exports = mongoose.model("upload", upload);
