const mongoose = require("mongoose");
var mongoosePaginate = require("mongoose-paginate");

const Schema = mongoose.Schema;

const project = new Schema(
	{
		name: { type: String },
		description: { type: String },
		// could aggregate across all dataproducts (optional)
		size: { type: Number },
		// what this product was originally derived from
		dataproducts: [{ type: Schema.Types.ObjectId, ref: "dataproduct" }],
		groups: [{ type: Schema.Types.ObjectId, ref: "Group" }],
		users: [{ type: Schema.Types.ObjectId, ref: "User" }],
		browser: { type: Boolean, default: false}
	},
	{ timestamps: true }
);

project.plugin(mongoosePaginate);

module.exports = mongoose.model("project", project);
