const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const clean = new Schema(
	{
		status: { type: String, default: "active" },
		dataproducts: [{ type: Schema.Types.ObjectId, ref: "dataproduct" }],
		datasets: [{ type: Schema.Types.ObjectId, ref: "dataset" }],
		tmp_dirs: [{ type: String }],
		size: { type: Number, default: 0 },
		error: [{ type: mongoose.Schema.Types.Mixed }],
	},
	{ timestamps: true }
);

module.exports = mongoose.model("clean", clean);
