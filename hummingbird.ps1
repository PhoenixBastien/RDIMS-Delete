using namespace Hummingbird.DM.Server.Interop.PCDClient

Add-Type -Path "C:\Program Files (x86)\Open Text\DM Extensions\Hummingbird.DM.Server.Interop.PCDClient.dll"

# input and log paths
$inputFilePath = ".\in\Copy of RDIMIS Migrated Documents.csv"
$logFilePath = ".\errors.log"
$credFilePath = ".\cred.xml"

# clean up old log
if (Test-Path $logFilePath) { Remove-Item $logFilePath }

# credentials
if (!(Test-Path $credFilePath)) {
    Get-Credential -Credential $Env:USERNAME | Export-Clixml $credFilePath
}
$cred = Import-Clixml $credFilePath

# login
$lib = "PS-SP"
$login = New-Object PCDLoginClass
$login.AddLogin(0, $lib, $cred.UserName, $cred.GetNetworkCredential().Password) | Out-Null
$login.Execute() | Out-Null
$dst = $login.GetDST()

# create search object to find document numbers
$search = New-Object PCDSearchClass
$search.SetDST($dst) | Out-Null
$search.AddSearchLib($lib) | Out-Null
$search.SetSearchObject("PS-SP_PROF_ML_2") | Out-Null
$search.AddReturnProperty("DOCNUM") | Out-Null

# reusable SQL object
$sql = New-Object PCDSQLClass
$sql.SetDST($dst) | Out-Null
$sql.SetLibrary($lib) | Out-Null

# chunk size of document numbers to search at once
$chunkSize = 10000

# get document numbers from input file
$toDelete = (Get-Content $inputFilePath -Raw).Trim() -split "[,\r\n]+"

for ($i = 0; $i -lt $toDelete.Count; $i += $chunkSize) {
    # split array into chunks of document numbers
    Write-Host "Searching for documents $($i + 1) to $($i + $chunkSize)..."
    $chunk = $toDelete[$i..($i + $chunkSize - 1)]
    $docNums = $chunk -join ","
    
    # batch update READONLY for docs
    $sql.Execute("UPDATE docsadm.profile SET readonly = 'N' WHERE docnumber IN ($docNums)") | Out-Null

    # batch update READONLY for parent docs
    $sql.Execute("UPDATE docsadm.profile SET readonly = 'N' WHERE docnumber IN (SELECT parent FROM docsadm.folder_item WHERE docnumber IN ($docNums))") | Out-Null

    # find document link ids
    $sql.Execute("SELECT system_id FROM docsadm.folder_item WHERE docnumber IN ($docNums)") | Out-Null

    do {
        # delete document link
        $linkId = $sql.GetColumnValue(1)
        $link = New-Object PCDDocObjectClass
        $link.SetDST($dst) | Out-Null
        $link.SetObjectType("ContentItem") | Out-Null
        $link.SetProperty("%TARGET_LIBRARY", $lib) | Out-Null
        $link.SetProperty("SYSTEM_ID", $linkId) | Out-Null
        $link.Delete() | Out-Null
    } while ($sql.NextRow())

    # release results
    $sql.ReleaseResults() | Out-Null

    # search for chunk of document numbers
    $search.AddSearchCriteria("DOCNUM", $docNums) | Out-Null
    $search.Execute() | Out-Null
    $count = $search.GetRowsFound()
    Write-Host "$count documents found"

    # begin get block
    $search.BeginGetBlock() | Out-Null

    while ($search.NextRow()) {
        # delete document
        $docNum = $search.GetPropertyValue("DOCNUM")
        $doc = New-Object PCDDocObjectClass
        $doc.SetDST($dst) | Out-Null
        $doc.SetObjectType("PS-SP_PROF_ML_2") | Out-Null
        $doc.SetProperty("%TARGET_LIBRARY", $lib) | Out-Null
        $doc.SetProperty("%OBJECT_IDENTIFIER", $docNum) | Out-Null
        $doc.Delete() | Out-Null

        # write to log if error occurs
        if ($doc.ErrNumber -ne 0) {
            "Doc # $docNum, $($doc.ErrDescription)".Trim() | Out-File -FilePath $logFilePath -Append
        }
    }

    # end get block and release results
    $search.EndGetBlock() | Out-Null
    $search.ReleaseResults() | Out-Null
}