module.exports = S3Simple;

var AWS = require('aws-sdk'),
    debug = require("debug")("tilelive-s3simple");

var s3 = new AWS.S3();

function S3Simple(uri, callback) {
    if (typeof uri === 'string') {
        uri = url.parse(uri, true);
    }

    this._uri = uri;
    this._stats = { get: 0, put: 0, noop: 0, txin: 0, txout: 0 };
    this._bucket = uri.host;
    this._prefix = uri.pathname;
    if(this._prefix.search("/") == 0) {
        this._prefix = this._prefix.substring(1);
    }
    this.filetype = "pbf";

    callback(null, this);

    return undefined;
}

S3Simple.registerProtocols = function(tilelive) {
    tilelive.protocols['s3simple:'] = S3Simple;
};


S3Simple.prototype.putTile = function(z, x, y, data, callback) {
    var key = this._getKey(z, x, y, this.filetype);
    debug("Putting tile to s3://" + this._bucket + "/" + key);

    s3.putObject({Bucket: this._bucket, Key:key, Body:data},
     function(err, data) {
        if (err) {
            callback(err);
        } else {
            callback(null);
        }
   });
};

S3Simple.prototype.getTile = function(z, x, y, data, callback) {
    var key = this._getKey(z, x, y, this.filetype);

    s3.getObject({Bucket: this._bucket, Key:key},
     function(err, data) {
        if (err) {
            callback(err);
        } else {
            callback(null, data);
        }
   });
};

S3Simple.prototype._getKey = function(z, x, y, filetype) {
    return this._prefix + z + "/" + x + "/" + y + "." + filetype;
};

S3Simple.prototype._getMetadataKey = function(e) {
    return this._prefix + "metadata.json";
};

S3Simple.prototype.stopWriting = function(callback) {
    callback(null);
}

S3Simple.prototype.startWriting = function(callback) {
    callback(null);
}

S3Simple.prototype.putInfo = function(info, callback) {
    var key = this._getMetadataKey();
    var data = JSON.stringify(info);

    s3.putObject({Bucket: this._bucket, Key:key, Body:data},
     function(err, data) {
        if (err) {
            callback(err);
        } else {
            callback(null);
        }
   });
};

S3Simple.prototype.getInfo = function(info, callback) {
    var key = this._getMetadataKey();

    s3.getObject({Bucket: this._bucket, Key:key},
     function(err, data) {
        if (err) {
            callback(err);
        } else {
            callback(null, data);
        }
   });
};
