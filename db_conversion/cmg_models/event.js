const mongoose = require("mongoose");
var mongoosePaginate = require("mongoose-paginate");

const Schema = mongoose.Schema;

const event = new Schema(
	{
		user: { type: Schema.Types.ObjectId, ref: "User" },
		action: { type: String },
		details: { type: String },
		// various "what"
		route: { type: String },
		dataproduct: { type: Schema.Types.ObjectId, ref: "dataproduct" },
		dataset: { type: Schema.Types.ObjectId, ref: "dataset" },
		project: { type: Schema.Types.ObjectId, ref: "project" },
		worker: { type: Schema.Types.ObjectId, ref: "worker" },
	},
	{ timestamps: true }
);

event.plugin(mongoosePaginate);

module.exports = mongoose.model("event", event);
