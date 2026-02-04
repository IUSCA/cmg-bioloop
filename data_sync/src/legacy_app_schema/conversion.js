const mongoose = require("mongoose");
// var mongoosePaginate = require("mongoose-paginate");
const Schema = mongoose.Schema;

const conversion = new Schema(
	{
		user: { type: Schema.Types.ObjectId, ref: "User" },
		dataset: { type: Schema.Types.ObjectId, ref: "dataset" },
		pipeline: { type: String },
		options: [{ type: String }],
		samplesheet: { type: String },
		worker: { type: Schema.Types.ObjectId, ref: "worker" },
		staged: { type: Boolean, default: false },
		output_path: { type: String },
		status: { type: String },
	},
	{ timestamps: true }
);

// conversion.plugin(mongoosePaginate);

module.exports = mongoose.model("conversion", conversion);
