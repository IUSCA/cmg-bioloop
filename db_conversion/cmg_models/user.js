const mongoose = require("mongoose");
const crypto = require("crypto");
var Schema = mongoose.Schema;

var userSchema = mongoose.Schema({
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	username: { type: String, index: { unique: true } },
	createDate: { type: Date, default: Date.now },
	lastLogin: { type: Date, default: Date.now },
	roles: [String],
	primary_role: String,
	fullname: String,
	email: String,
	notifications: { type: Boolean, default: true },
	active: { type: Boolean, default: true },
	prefs: mongoose.Schema.Types.Mixed,
	hash: String,
	salt: String,
});

userSchema.methods.setPassword = function (password) {
	this.salt = crypto.randomBytes(16).toString("hex");
	this.hash = crypto
		.pbkdf2Sync(password, this.salt, 10000, 512, "sha512")
		.toString("hex");
};

userSchema.methods.validatePassword = function (password) {
	const hash = crypto
		.pbkdf2Sync(password, this.salt, 10000, 512, "sha512")
		.toString("hex");
	return this.hash === hash;
};

exports.User = mongoose.model("User", userSchema);
