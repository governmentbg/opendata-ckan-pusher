commons-daemon\prunsrv //IS//OpenDataPusher --DisplayName="OpenData Pusher" --Description="OpenData Pusher"^
     --Install="%cd%\commons-daemon\prunsrv.exe" --Jvm="%cd%\jre1.8.0_91\bin\client\jvm.dll" --StartMode=jvm --StopMode=jvm^
     --Startup=auto --StartClass=bg.government.opendatapusher.Pusher --StopClass=bg.government.opendatapusher.Pusher^
     --StartParams=start --StopParams=stop --StartMethod=windowsService --StopMethod=windowsService^
     --Classpath="%cd%\opendata-ckan-pusher.jar" --LogLevel=DEBUG^ --LogPath="%cd%\logs" --LogPrefix=procrun.log^
     --StdOutput="%cd%\logs\stdout.log" --StdError="%cd%\logs\stderr.log"
     
     
commons-daemon\prunsrv //ES//OpenDataPusher