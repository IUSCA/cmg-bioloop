const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const content = new Schema(
	{
		// distinguish between different content blurbs
		name: { type: String },
		// aka the content for rendering
		details: { type: String },
	},
	{ timestamps: true }
);

module.exports = mongoose.model("content", content);
