using namespace Hummingbird.DM.Server.Interop.PCDClient

Add-Type -Path "C:\Program Files (x86)\Open Text\DM Extensions\Hummingbird.DM.Server.Interop.PCDClient.dll"

# get document numbers from input file
$inputFilePath = ".\in\RDIMIS Migrated Documents.csv"
$toDelete = $(Get-Content $inputFilePath) -split ","

# remove previous log file if it exists
$logFilePath = ".\errors.log"
if (Test-Path $logFilePath) {
    Remove-Item $logFilePath
}

# create credential file if it doesn't exist
$credFilePath = ".\cred.xml"
if (!(Test-Path $credFilePath)) {
    Get-Credential -Credential $Env:USERNAME | Export-Clixml $credFilePath
}
$cred = Import-Clixml $credFilePath

# log into edocs account and get dst
$lib = "PS-SP"
$login = New-Object PCDLoginClass
$login.AddLogin(0, $lib, $cred.UserName, $cred.GetNetworkCredential().Password) +
$login.Execute() | Out-Null
$dst = $login.GetDST()

# create search object to find document numbers
$search = New-Object PCDSearchClass
$search.SetDST($dst) +
$search.AddSearchLib($lib) +
$search.SetSearchObject("PS-SP_PROF_ML_2") +
$search.AddReturnProperty("DOCNUM") | Out-Null

# chunk size of document numbers to search at once
$chunkSize = 10000

for ($i = 0; $i -lt $toDelete.Count; $i += $chunkSize) {
    # split array into chunks of document numbers
    Write-Host "Searching for documents from $i to $($i + $chunkSize - 1)..."
    $chunk = $toDelete[$i..($i + $chunkSize - 1)]

    # search for chunk of document numbers
    $search.AddSearchCriteria("DOCNUM", $chunk -join ",") +
    $search.Execute() | Out-Null

    # get number of documents found
    $count = $search.GetRowsFound()
    Write-Host "$count documents found"

    # begin get block
    $search.BeginGetBlock() | Out-Null

    while ($search.NextRow()) {
        # get document by document number and remove read-only
        $docNum = $search.GetPropertyValue("DOCNUM")
        $doc = New-Object PCDDocObjectClass
        $doc.SetDST($dst) +
        $doc.SetObjectType("PS-SP_PROF_ML_2") +
        $doc.SetProperty("%TARGET_LIBRARY", $lib) +
        $doc.SetProperty("%OBJECT_IDENTIFIER", $docNum) +
        $doc.SetProperty("READONLY", "N") +
        $doc.Update() | Out-Null

        # find document link ids and parent document numbers
        $sql = New-Object PCDSQLClass
        $sql.SetDST($dst) +
        $sql.SetLibrary($lib) +
        $sql.Execute("select parent, system_id from docsadm.folder_item where docnumber = $docNum") | Out-Null

        for ($j = 0; $j -lt $sql.GetRowCount(); $j++) {
            $parentDocNum = $sql.GetColumnValue(1)
            $parentDoc = New-Object PCDDocObjectClass
            $parentDoc.SetDST($dst) +
            $parentDoc.SetObjectType("PS-SP_PROF_ML_2") +
            $parentDoc.SetProperty("%TARGET_LIBRARY", $lib) +
            $parentDoc.SetProperty("%OBJECT_IDENTIFIER", $parentDocNum) +
            $parentDoc.SetProperty("READONLY", "N") +
            $parentDoc.Update() | Out-Null

            # get document link by link id and delete document link
            $linkId = $sql.GetColumnValue(2)
            $link = New-Object PCDDocObjectClass
            $link.SetDST($dst) +
            $link.SetObjectType("ContentItem") +
            $link.SetProperty("%TARGET_LIBRARY", $lib) +
            $link.SetProperty("SYSTEM_ID", $linkId) +
            $link.Delete() | Out-Null

            # continue to next document link
            $sql.NextRow() | Out-Null
        }

        # release results and delete document
        $sql.ReleaseResults() +
        $doc.Delete() | Out-Null

        # write to log if error occurs
        if ($doc.ErrNumber -ne 0) {
            "Doc # $docNum, $($doc.ErrDescription)" | Out-File -FilePath $logFilePath -Append
        }
    }

    # end get block and release results
    $search.EndGetBlock() +
    $search.ReleaseResults() | Out-Null
}