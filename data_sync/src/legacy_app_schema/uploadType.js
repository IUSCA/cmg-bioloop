const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const uploadType = new Schema({
    name: { type: String, required: true },
    extension: { type: String, required: true },
    displayOrder: { type: Number, required: true }
}, { timestamps: true });

module.exports = mongoose.model('uploadType', uploadType);
