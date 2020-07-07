
$arangosh='arangosh.exe'
if (Test-Path 'Env:ARANGOSH') {
  $arangosh=$Env:ARANGOSH
}

$scriptArgs = '--javascript.execute ./index.js '
if( $args.Count -eq 0 -or $args[0] -eq "help") {
  $scriptArgs += '--server.endpoint none '
  
}
$scriptArgs += $args
Start-Process -Wait -WorkingDirectory './debugging' -NoNewWindow -FilePath  $arangosh -ArgumentList $scriptArgs 

