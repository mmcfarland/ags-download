# ags-download

Download layers or a map service from an ESRI ArcGIS server as geoJSON.

### Installation
 ``` 
 > npm install ags-download
 ```

### Usage
```
> ags-download -u <ags_layer_url> -f <path_to_save> [-s <output_srid>]
```
#### Examples

Download all layers in a MapServer to the current directory with source srid
```
> ags-download -u http://gis.phila.gov/ArcGIS/rest/services/PhilaGov/RCO/MapServer 
```

Download individual layer to /tmp with srid of EPSF:4326
```
> ags-download -u http://gis.phila.gov/ArcGIS/rest/services/PhilaGov/RCO/MapServer/0 -f /tmp -s 4326
```

## License
Copyright (c) 2013 Matthew McFarland  
Licensed under the MIT license.
