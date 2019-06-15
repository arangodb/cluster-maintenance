#!/bin/bash

arangosh \
    --server.endpoint none \
    --javascript.execute lib/analyze.js
