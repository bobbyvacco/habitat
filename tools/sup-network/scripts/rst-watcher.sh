#!/bin/bash

watch -n 1 "rst-reader $(find /hab/sup/default/data -iname "*.rst")"
