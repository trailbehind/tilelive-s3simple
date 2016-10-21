module.exports = S3Simple;

var AWS = require('aws-sdk'),
    debug = require("debug")("tilelive-s3simple"),
    error = require("debug")("tilelive-s3simple:error");

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
    this.filetype = uri.query.filetype || 'png';
    this.contentEncoding = uri.query.contentencoding;
    this.contentType = uri.query.contenttype;

    callback(null, this);

    return undefined;
}

S3Simple.registerProtocols = function(tilelive) {
    tilelive.protocols['s3simple:'] = S3Simple;
};


S3Simple.prototype.putTile = function(z, x, y, data, callback) {
    var key = this._getKey(z, x, y, this.filetype),
        bucket = this._bucket,
        params = {Bucket:bucket, Key:key, Body:data};
    debug("Putting tile to s3://" + bucket + "/" + key);

    if(this.contentEncoding) {
        params.ContentEncoding = this.contentEncoding;
    }
    if(this.contentType) {
        params.ContentType = this.contentType;
    }
    s3.putObject(params,
     function(err, data) {
        if (err) {
            error("Error put s3://" + bucket + "/" + key + "\n" + err);
            callback(err);
        } else {
            debug("Finish put s3://" + bucket + "/" + key);
            callback(null);
        }
   });
};

S3Simple.prototype.getTile = function(z, x, y, data, callback) {
    var key = this._getKey(z, x, y, this.filetype),
        bucket = this._bucket;
    s3.getObject({Bucket:bucket, Key:key},
     function(err, data) {
        if (err) {
            error("Error get s3://" + bucket + "/" + key + "\n" + err);
            callback(err);
        } else {
            debug("Finish get s3://" + bucket + "/" + key);
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
    var key = this._getMetadataKey(),
        bucket = this._bucket,
        data = JSON.stringify(info);

    s3.putObject({Bucket:bucket, Key:key, Body:data},
     function(err, data) {
        if (err) {
            error("Error put s3://" + bucket + "/" + key + "\n" + err);
            callback(err);
        } else {
            debug("Finish put s3://" + bucket + "/" + key);
            callback(null);
        }
   });
};

S3Simple.prototype.getInfo = function(info, callback) {
    var key = this._getMetadataKey(),
        bucket = this._bucket;

    s3.getObject({Bucket:bucket, Key:key},
     function(err, data) {
        if (err) {
            error("Error get s3://" + bucket + "/" + key + "\n" + err);
            callback(err);
        } else {
            debug("Finish get s3://" + bucket + "/" + key);
            callback(null, data);
        }
   });
};
