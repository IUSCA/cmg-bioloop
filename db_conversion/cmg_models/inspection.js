const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const inspection = new Schema(
	{
		name: { type: String },
		original_path: { type: String },
		size: { type: Number },
		files: { type: Number },
		cbcls: { type: Number },
		directories: { type: Number },
	},
	{ timestamps: true }
);

module.exports = mongoose.model("inspection", inspection);
