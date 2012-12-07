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
        .alias('s', 'srid')
        .describe('s', 'Output Spatial Reference Id')
        .default('s', null)
		.argv,

	request = require('request'),
	async = require('async'),
	url = require('url'),
	geo = require('geojson'),
	fs = require('fs'),
	util = require('util'),
    _ = require('underscore'),

	geomType = {
		'esriGeometryPolygon': 'Polygon',
		'esriGeometryPoint': 'Point'
	};

var input = cleanUrl(argv.u),
	chunkSize = 500,
	chunkThreshold = 10,
	queryOpts = {
		outFields: '*',
		f: 'json',
		returnGeometry: true,
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

        } else if (body.capabilities.split(',').some(function(ability) {return ability === 'Query';})) {
            // Otherwise, a single layer and it must support Query
            downloadLayer({uri: layerUrl, geometryType: body.geometryType}, function(geoJson) {
                if (argv.file) {
                    saveFile(geoJson, body.id + '_' + body.name + '.json');
                } else if (argv.cartodb) {
                    //
                }
            });
        } else {
            throw "MapService does not support Query interface";
        }

    });
}

requestLayer(input);


function downloadLayer(options, callback) {
	// AGS implementations will set a limit on the number of records returned
	// by the query operation.  To retrieve all records, first get a list of
	// all ids, which do not have this restriction, then download in chunks
	getObjectIds(options, function handleIdsReceived(err, resp, body) {
		if (err) throw err;
		if (resp.statusCode === 200 && body.objectIdFieldName) {
			chunk(options, body, callback);
		} else {
			console.log(body);
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
	var numberOfRequests = Math.ceil(idList.objectIds.length/chunkSize);

	if (numberOfRequests <= chunkThreshold || argv.force) {
		var s=0, e=chunkSize -1, reqs = [];
		for (var i=0; i < numberOfRequests; i++) {
			reqs.push(makeQuery(options, idList, s, e));
			s += chunkSize;
			e += chunkSize;
		}

		async.series(reqs, function createGeoJson(err, attributes) {
			jsonOpts = {};
			jsonOpts[geomType[options.geometryType]] = 'geom';
			all = [].concat.apply([], attributes);
			geo.parse(all, jsonOpts, callback);
		});
	} else {
		// TODO: Make --force option
		console.log('Current settings would require ' + numberOfRequests + ' requests to ' +
			'this AGS, and might be a lot of data.  At the moment, this is not supported.');
	}
}

function makeQuery(options, idList, start, end) {
	return function(callback) {
		console.log('requesting ' + start + ' '  + end);
		query({
			uri: url.resolve(options.uri, 'query'),
			json: true,
			qs:_.extend(queryOpts, {
				where: util.format('%s BETWEEN %s AND %s', idList.objectIdFieldName,
					idList.objectIds[start],
					idList.objectIds[end] || idList.objectIds[idList.objectIds.length-1])
			})
		}, function handleFeatureResponse(error, resp, body) {
			if (error) {throw error;}
			
			if (resp.statusCode === 200 && body.features) {
				callback(null, body.features.map(esriFeatureToGeoJson(body.geometryType)));
			} else {
				console.log(body);
				throw "Invalid response: " + body;
			}
		});
	};
}

function query(options, callback) {
	request.get(options, callback);
}

function cleanUrl(url) {
    if (url[url.length-1] === '/') {
        return url;
    } else {
        return url + '/';
    }
}

function esriFeatureToGeoJson(esriGeomType) {
	return function convertGeom(feature) {
		var attrs = feature.attributes;

		switch (esriGeomType) {
			case 'esriGeometryPolygon':
				attrs.geom = feature.geometry.rings;
				break;
			case 'esriGeometryPoint':
				attrs.geom = [feature.geometry.x, feature.geometry.y];
				break;
			case 'esriGeometryPolyline':
				attrs.geom = feature.geometry.paths;
				break;
		}

		return attrs;
	};
}


function saveFile(geoJson) {
	fs.writeFile(argv.file, JSON.stringify(geoJson), function(err) {
		if (err) throw err;
		console.log('saved.');
	});
}