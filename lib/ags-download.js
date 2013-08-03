#!/usr/bin/env node

/*
 * ags-download
 * https://github.com/mmcfarland/ags-download
 *
 * Copyright (c) 2012 Matthew McFarland
 * Licensed under the MIT license.
 */

var argv = require('optimist')
		.usage('Download data from an ArcGIS Server as geoJSON')
		.demand('u')
		.alias('u', 'url')
		.describe('u', 'URL of the ArcGIS MapServer layer to download (must support Query interface)')

        .demand('f')
		.alias('f', 'file')
        .describe('f', 'Path to save resulting geoJSON file')
        .default('f', '.')

        .alias('s', 'srid')
        .describe('s', 'Output Spatial Reference Id')
        .default('s', 4326)

        .alias('o', 'output')
        .describe('o', 'Output fields')
        .default('o', '*')

        .boolean('n', 'no-geometry')
        .describe('n', 'Do not return geometry')
		.argv,

    Writable = require('stream').Writable,
    path = require('path'),
	request = require('request'),
	async = require('async'),
	url = require('url'),
	fs = require('fs'),
	util = require('util'),
    _ = require('underscore'),
    esriConverter = require('../libs/esriJsonConverters.js').esriConverter(),
    _wkid,
    _ws;

var input = cleanUrl(argv.u),
	chunkSize = 500,
	chunkThreshold = 10000,
	queryOpts = {
		outFields: argv.o,
		f: 'json',
		returnGeometry: !argv.n,
        outSR: argv.srid
	};

function requestLayer(layerUrl) {
	capabilityOpts = {
		uri: url.resolve(layerUrl, '?f=json'),
		json: true
	};

    request.get(capabilityOpts, function(error, resp, body) {
        if (error) {throw error;}

        if (body.layers) {
            // This is a layer group
            body.layers.forEach(function downloadEachLayer( layer) {
                requestLayer(url.resolve(layerUrl, layer.id.toString() + '/'));
            });

        } else if (body && body.capabilities &&
            body.capabilities.split(',').some(function(ability) {return ability === 'Query';})) {
            // Otherwise, a single layer and it must support Query
            var filepath = path.resolve(argv.f, body.id + '_' + body.name + '.json');
            if (fs.existsSync(filepath)) fs.unlink(filepath);
            var ws = fs.createWriteStream(filepath);
            ws.write('{ "type": "FeatureCollection", "features": [');
            downloadLayer({uri: layerUrl, geometryType: body.geometryType, writer: ws}, function(geoJson) {
                if (argv.file) {
                    ws.end(']}');
                } else if (argv.cartodb) {
                    //
                }
            });
        } else {
            throw "MapService does not support Query interface";
        }

    });
}


function downloadLayer(options, callback) {
	// AGS implementations will set a limit on the number of records returned
	// by the query operation.  To retrieve all records, first get a list of
	// all ids, which do not have this restriction, then download in chunks
	getObjectIds(options, function handleIdsReceived(err, resp, body) {
		if (err) throw err;
		if (resp.statusCode === 200 && body.objectIdFieldName) {
            var ids = body.objectIds.sort(function(a,b) { return a-b;});
			chunk(options, ids, callback);
		} else {
			throw "Could not retrieve feature ObjectIds";
		}
	});
}

function getObjectIds(options, callback) {
	request.get({
		uri: url.resolve(options.uri, 'query'),
		json: true,
		qs: {
			returnIdsOnly: true,
			where: '1=1',
			f: 'json'
		}
	}, callback);
}

function chunk(options, idList, callback) {
	// To be considerate of the the poor AGS that is hosting these files
	// make the chunk requests serially.  Also, if the number of requests
	// is going to exceed {{threshold}}, require the user to use 'force'
	var numberOfRequests = Math.ceil(idList.length/chunkSize);

	if (numberOfRequests <= chunkThreshold || argv.force) {
		var s=0, e=chunkSize -1, reqs = [];
		for (var i=0; i < numberOfRequests; i++) {
			reqs.push(makeQuery(options, idList, s, e));
			s += chunkSize;
			e += chunkSize;
		}

		async.series(reqs, function createGeoJson(err, attributes) {
			all = [].concat.apply([], attributes);
            var crs = {type:"name", properties: {name: "EPSG:" + _wkid}},
                geo = {type: "FeatureCollection", features: all, crs: crs};
			callback(geo);
		});
	} else {
		// TODO: Make --force option
		console.log('Current settings would require ' + numberOfRequests + ' requests to ' +
			'this AGS, and might be a lot of data.  At the moment, this is not supported.');
	}
}

function makeQuery(options, idList, start, end) {
	return function(callback) {
        var qs = _.extend(queryOpts, {
            where: util.format('OBJECTID BETWEEN %s AND %s',
                idList[start],
                idList[end] || idList[idList.length-1]
                )
        });
		request.get({
			uri: url.resolve(options.uri, 'query'),
			json: true,
			qs: qs
		}, function handleFeatureResponse(error, resp, body) {
			if (error) {throw error;}
            // Best guess as to the wkid for geojson
			_wkid = _wkid || (body.spatialReference ? body.spatialReference.wkid : null) || argv.srid;

			if (resp.statusCode === 200 && body.features) {
                var hasFeatures = !!body.features.length,
                    firstComma = start === 0 ? "" : ",",
                    jstr;
       
                if (hasFeatures && qs.returnGeometry) {
                    jstr = JSON.stringify(body.features.map(esriConverter.toGeoJson));
                } else if (hasFeatures) {
                    // Don't convert to geojson if there is no geo
                    jstr = JSON.stringify(body.features);
                } else {
                    return;
                } 
                options.writer.write(firstComma + jstr.substring(1, jstr.length-1) + '\n');
                callback(null);
			} else {
				callback("Invalid response: " + body);
			}
		});
	};
}

function cleanUrl(url) {
    if (url[url.length-1] === '/') {
        return url;
    } else {
        return url + '/';
    }
}

// Start the request
try {
    requestLayer(input);
} catch (e) {
    console.log("Could not complete request:")
    console.log(e);
}
