var events = require('events')
  , uuid = require('node-uuid')
  , util = require('util')
  , events = require('events')
  , levelup = require('levelup')
  , stream = require('stream')
  , difference = require('lodash.difference')
  , mapreduce = require('../level-mapreduce')
  , once = require('once')
  , noop = function () {}
  ;

function Index (lev, name, map, opts) {
  var self = this
  if (typeof lev === 'string') lev = levelup(lev, {keyEncoding:'binary', valueEncoding:'json'})
  self.lev = lev
  self.name = name
  self.map = map

  function lmap (chunk, cb) {
    if (self.async) {
      self.map(chunk, function (e, result) {
        if (e) return cb(e)
        cb(null, result.map(function (t) {return [t[1], t[0]]}))
      })
    } else {
      return map(chunk).map(function (t) {return [t[1], t[0]]})
    }
  }

  self.leftIndex = mapreduce(lev, ['lmesh', name], lmap)
  self.rightIndex = mapreduce(lev, ['rmesh', name], self.map)

  self.opts = opts || {}
  // if (!this.opts.key) this.opts.key = function (entry) { return entry.key }
  stream.Transform.call(self, {objectMode:true})
}
util.inherits(Index, stream.Transform)
// Index.prototype.createReadStream = function (opts) {
//   if (!opts) opts = {}
//   if (!opts.raw) {
//     if (opts.start) opts.start = this.bytes.encode(['index', opts.start])
//     if (opts.end) opts.end = this.bytes.encode(['index', opts.end])
//     if (!opts.start) opts.start = this.bytes.encode(['index'])
//     if (!opts.end) opts.end = this.bytes.encode(['index', {}])
//   }
//
//   return this.mutex.lev.createReadStream(opts).pipe(new DecodeStream(this.bytes.decode.bind(this.bytes)))
// }

Index.prototype.getLeft = function (key, cb) {
  this.leftIndex.get(key, cb)
}
Index.prototype.getRight = function (key, cb) {
  this.rightIndex.get(key, cb)
}
Index.prototype.get = function (key, opts, cb) {
  var left
    , right
    ;
  cb = once(cb)
  this.getLeft(key, function (e, results) {
    if (e) return cb(e)
    left = results
    if (right) cb(null, {left:left, right:right})
  })
  this.getRight(key, function (e, results) {
    if (e) return cb(e)
    right = results
    if (left) cb(null, {left:left, right:right})
  })
}
Index.prototype.leftMesh = function (key, cb) {
  var self = this
    , index = {}
    , allkeys = [key]
    , pending = 0
    ;
  function _get (_key) {
    pending = pending + 1
    self.getLeft(_key, function (e, r) {
      pending = pending - 1
      index[_key] = r
      var newkeys = difference(r, allkeys)
      newkeys.forEach(_get)
      allkeys = allkeys.concat(newkeys)
      if (pending === 0) finish()
    })
  }
  _get(key)
  function finish () {
    var _d = []
      , _i = {}
      , mesh = {}
      ;
    function mesher (k, m) {
      index[k].forEach(function (k) {
        if (_d.indexOf(k) === -1) {
          m[k] = {}
          _i[k] = m[k]
          _d.push(k)
          mesher(k, m[k])
        } else {
          m[k] = _i[k]
        }
      })
    }
    mesher(key, mesh)

    cb(null, {keys:allkeys, index:index, mesh:mesh})
  }
}


Index.prototype._transform = function (chunk, encoding, cb) {
  var self = this

  var count = 2
    , left
    , right
    ;

  function callback (e) {
    count = count - 1
    if (count === 0) {
      if (self._piped) self.push({key:chunk.key, value:{left:left, right: right}})
      cb()
    }
  }

  self.leftIndex.write(chunk, encoding, function (e, r) {
    self.leftIndex.getMeta(chunk.key, function (e, meta) {
      left = meta.value ? meta.value : null
      callback(e)
    })
  })
  self.rightIndex.write(chunk, encoding, function (e, r) {
    self.rightIndex.getMeta(chunk.key, function (e, meta) {
      right = meta.value ? meta.value : null
      callback(e)
    })
  })

}
Index.prototype.pipe = function () {
  this._piped = true
  stream.Transform.prototype.pipe.apply(this, arguments)
}

// Index.prototype.write = function (entry, ref, tuples, cb) {
// }

function AsyncIndex () {
  this.async = true
  Index.apply(this, arguments)
}
util.inherits(AsyncIndex, Index)

module.exports = function (lev, name, map, opts) { return new Index(lev, name, map, opts) }
module.exports.async = function (lev, name, map, opts) {
  return new AsyncIndex(lev, name, map, opts)
}
