#
#                        __  __                                 
#                       |  \/  |                                
#          _ __    ___  | \  / |   ___    _ __     __ _    ___  
#         | '_ \  / __| | |\/| |  / _ \  | '_ \   / _` |  / _ \ 
#         | |_) | \__ \ | |  | | | (_) | | | | | | (_| | | (_) |
#         | .__/  |___/ |_|  |_|  \___/  |_| |_|  \__, |  \___/ 
#         | |                                      __/ |        
#         |_|                                     |___/         
#
#
#
#
#M@

<#

        *                                                  *
        ~ This module is designed to interact with MongoDB ~
        *                                                  *

        LICENSE INFO:
        Copyright $(Get-Date) (c) M@. 

        All rights reserved.

        MIT License

        Permission is hereby granted", "free of charge", "to any person obtaining a copy
        of this software and associated documentation files (the ""Software"")", "to deal
        in the Software without restriction", "including without limitation the rights
        to use", "copy", "modify", "merge", "publish", "distribute", "sublicense", "and/or sell 
        copies of the Software", "and to permit persons to whom the Software is
        furnished to do so", "subject to the following conditions:

        The above copyright notice and this permission notice shall be included in all
        copies or substantial portions of the Software.

        THE SOFTWARE IS PROVIDED *AS IS*", "WITHOUT WARRANTY OF ANY KIND", "EXPRESS OR
        IMPLIED", "INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
        FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM", "DAMAGES OR OTHER
        LIABILITY", "WHETHER IN AN ACTION OF CONTRACT", "TORT OR OTHERWISE", "ARISING FROM,
        OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
        SOFTWARE.

        IMPORTANT NOTICE:  THE SOFTWARE ALSO CONTAINS THIRD PARTY AND OTHER
        PROPRIETARY SOFTWARE THAT ARE GOVERNED BY SEPARATE LICENSE TERMS. BY ACCEPTING
        THE LICENSE TERMS ABOVE", "YOU ALSO ACCEPT THE LICENSE TERMS GOVERNING THE
        THIRD PARTY AND OTHER SOFTWARE", "WHICH ARE SET FORTH IN ThirdPartyNotices.txt

#>




$Global:MONGO_SESSION_INFO = @{
    "ImportedDlls" = "null"
}
 
function Import-MongoDLLs
{
    param(
        [Parameter(Mandatory=$true)]
        [ValidateScript({ (([System.IO.File]::Exists($_)) -and ($_.EndsWith("MongoDB.Bson.dll")))
                    
                    "MongoDB.Bson.dll EITHER DOES NOT END IN DLL OR DOES NOT EXIST!"
        
        })]
        [string]$MongoDBBsonDllLocation,
        
        [Parameter(Mandatory=$true)]
        [ValidateScript({ (([System.IO.File]::Exists($_)) -and ($_.EndsWith("MongoDB.Driver.Core.dll")))
        
                    "MongoDB.Driver.Core.dll EITHER DOES NOT END IN DLL OR DOES NOT EXIST!"
        
        })]
        [string]$MongoDBDriverCoreDllLocation,
        
        [Parameter(Mandatory=$true)]
        [ValidateScript({ (([System.IO.File]::Exists($_)) -and ($_.EndsWith("MongoDB.Driver.dll")))
        
                    "MongoDB.Driver.dll EITHER DOES NOT END IN DLL OR DOES NOT EXIST!"
        
        })]
        [string]$MongoDBDriverDllLocation                
    )
    
    try { 
            
        ##########################################################################################################
        # SOURCE
        ##########################################################################################################
        $csharpcode = @"
using System;
using System.Linq;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using System.Collections.Generic;

namespace PsMongo
{
    public class PsMongoContext
    {
        public string MongoServer { get; set; }
        public string MongoDatabaseName { get; set; }
        public bool MongoIsConnected { get; set; }
        private string MongoUsername { get; set; }
        private string MongoPassword { get; set; }
        private IMongoClient IMongoClient { get; set; }
        private IMongoDatabase IMongoDatabase { get; set; }
        

        public PsMongoContext(string mongoServer)
        {
            this.MongoServer = mongoServer;
            this.BindToMongo(mongoServer);
        }

        public PsMongoContext(string mongoServer, string databaseName)
        {
            this.MongoServer = mongoServer;
            this.MongoDatabaseName = databaseName;
            this.BindToMongo(mongoServer, databaseName);
        }

        public PsMongoContext(string mongoServer, string localmongousername, string localmongopassword)
        {
            this.MongoServer = mongoServer;
            this.MongoUsername = localmongousername;
            this.MongoPassword = localmongopassword;
            this.BindToMongo(mongoServer, localmongousername, localmongopassword);
        }

        public PsMongoContext(string mongoServer, string databaseName, string localmongousername, string localmongopassword)
        {
            this.MongoServer = mongoServer;
            this.MongoDatabaseName = databaseName;
            this.MongoUsername = localmongousername;
            this.MongoPassword = localmongopassword;
            this.BindToMongo(mongoServer, databaseName, localmongousername, localmongopassword);
        }

        private PsMongoContext BindToMongo(string server)
        {
            try
            {
                var connectionString = string.Format("mongodb://{0}:27017", server);
                this.IMongoClient = new MongoClient(connectionString);
                this.MongoIsConnected = true;
                return this;
            }
            catch
            {
                this.MongoIsConnected = false;
                return null;
            }
        }

        private PsMongoContext BindToMongo(string server, string databasename)
        {
            try
            {
                var connectionString = string.Format("mongodb://{0}:27017/{1}", server, databasename);
                this.IMongoClient = new MongoClient(connectionString);
                this.IMongoDatabase = this.IMongoClient.GetDatabase(databasename);
                this.MongoIsConnected = true;
                return this;
            }
            catch
            {
                this.MongoIsConnected = false;
                return null;
            }
        }

        private PsMongoContext BindToMongo(string server, string localmongousername, string localmongopassword)
        {
            try
            {
                var connectionString = string.Format("mongodb://{0}:{1}@{2}:27017", localmongousername, localmongopassword, server);
                this.IMongoClient = new MongoClient(connectionString);
                this.MongoIsConnected = true;
                return this;
            }
            catch
            {
                this.MongoIsConnected = false;
                return null;
            }
        }

        private PsMongoContext BindToMongo(string server, string databasename, string localmongousername, string localmongopassword)
        {
            try
            {
                var connectionString = string.Format("mongodb://{0}:{1}@{2}:27017/{3}", localmongousername, localmongopassword, server, databasename);
                this.IMongoClient = new MongoClient(connectionString);
                this.IMongoDatabase = this.IMongoClient.GetDatabase(databasename);
                this.MongoIsConnected = true;
                return this;
            }
            catch
            {
                this.MongoIsConnected = false;
                return null;
            }
        }

        private bool BindToMongoDatabase(string dbname)
        {
            if (this.MongoServer == null)
            {
                throw new Exception("In order to connect to a database you must first connect to a server.");
            }
            else
            {
                this.BindToMongo(this.MongoServer, dbname);
                this.IMongoDatabase = this.IMongoClient.GetDatabase(dbname);
                this.MongoDatabaseName = dbname;
                return true;
            }
        }

        public bool ConnectToMongoDatabase(string databasename)
        {
            return this.BindToMongoDatabase(databasename);
        }
        
        public bool CreateNewDatabase(string databaseName, string collectionName) // each database must be created with a collection or else empty databases remove themselves
        {
            try
            {
                // even tho this says get database, if the database doesnt exist - it cretes it
                var newDB = this.IMongoClient.GetDatabase(databaseName);
                newDB.CreateCollection(collectionName);
                return true;
            }
            catch (Exception e)
            {
                throw new Exception("Something went wrong creating database. Full Error\r\n\r\n" + e.Message);
            }
        }

        public bool RemoveDatabase(string databaseName)
        {
            try
            {
                this.IMongoClient.DropDatabase(databaseName);
                return true;
            }
            catch (Exception e)
            {
                throw new Exception("Something went wrong while removing database '" + databaseName + "'.\r\nFull Error\r\n\r\n" + e.Message);
            }
        }

        public bool CreateNewCollection(string collectionName)
        {
            try
            {
                this.IMongoDatabase.CreateCollection(collectionName);
                return true;
            }
            catch (Exception e)
            {
                throw new Exception("Something went wrong creating database. Full Error\r\n\r\n" + e.Message);
            }
        }

        public bool RemoveCollection(string collectionName)
        {
            try
            {
                this.IMongoDatabase.DropCollection(collectionName);
                return true;
            }
            catch (Exception e)
            {
                throw new Exception("Something went wrong while removing collection '" + collectionName + "'.\r\nFull Error\r\n\r\n" + e.Message);
            }
        }

        public List<BsonDocument> GetAllDocumentsFromCollection(string collectionName)
        {
            try
            {
                var collection = this.IMongoDatabase.GetCollection<BsonDocument>(collectionName);
                return collection.Find(Builders<BsonDocument>.Filter.Empty).ToList();
            }
            catch (Exception e)
            {
                throw new Exception("Something went wrong while gather documents from collection: '" + collectionName + "'!\r\nFull Error:\r\n\r\n" + e.Message);
            }
        }

        public IMongoCollection<BsonDocument> GetMongoCollection(string collectionName)
        {
            try
            {
                return this.IMongoDatabase.GetCollection<BsonDocument>(collectionName);
            }
            catch (Exception e)
            {
                throw new Exception("Something went wrong while locating collection: '" + collectionName + "'!\r\nFull Error:\r\n\r\n" + e.Message);
            }
        }

        public bool InsertDocumentIntoMongoCollection(string json, string collectionName)
        {
            try
            {
                var collection = this.IMongoDatabase.GetCollection<BsonDocument>(collectionName);
                var bson_ = json.ToBsonDocument();
                collection.InsertOne(bson_);
                return true;
            }
            catch (Exception e)
            {
                throw new Exception("Something went wrong while inserting document into collection!\r\n\r\nFull Error:\r\n\r\n" + e.Message);
            }
        }

        public bool RemoveDocumentFromMongoCollection(string jsonDocument, string collectionName)
        {
            try
            {
                var collection = this.IMongoDatabase.GetCollection<BsonDocument>(collectionName);
                var doc = MongoConverter.JSONtoBSON(jsonDocument);
                if (doc == null)
                {
                    return false;
                }
                else
                {
                    collection.DeleteOne(doc);
                    return true;
                }
            }
            catch (Exception e)
            {
                var m = string.Format("Something went wrong while removing document from collection! Full Error:\r\n\r\n{0}", e.Message);
                throw new Exception(m);
            }
        }
    }

    public class MongoConverter
    {
        public static string BSONtoJSON(BsonDocument bson)
        {
            // JsonOutputMode.Strict is what lets us pull from MongoDB straight into PSCustomObject
            return bson.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.Strict });
        }

        public static BsonDocument JSONtoBSON(string json)
        {
            return MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(json);
        }
    }

}
"@
        ##########################################################################################################
        # ADD MONGO DLLS
        ##########################################################################################################
        $references = @($MongoDBBsonDllLocation, $MongoDBDriverCoreDllLocation, $MongoDBDriverDllLocation)
        $references | ForEach-Object { 
            $null = [System.Reflection.Assembly]::LoadFile($_) 
        }
        ##########################################################################################################
        # ADD CUSTOM CSHARP CODE TO POSH SESSION
        ##########################################################################################################
        Add-Type -TypeDefinition $csharpcode -ReferencedAssemblies $references
        $Global:MONGO_SESSION_INFO.ImportedDlls = $true
        ##########################################################################################################    
        
    } catch {
    
        $Global:MONGO_SESSION_INFO.ImportedDlls = $false
        Write-Host "`r`n               Unable to import required MongoDB DLL files!                `r`n`r`nFull Error:`r`n`r`n$($_)`r`n`r`n" -f Red -b Black
        
    }
    
}

function New-MongoConnection
{
    # If database name is empty, mongo will connect to the admin db by default
    [cmdletbinding(DefaultParameterSetName="DFLT")]
    param(
        [Parameter(Mandatory=$true)]
        [string]$MongoServerHostname,
        
        [Parameter(Mandatory=$false)]
        [string]$MongoDatabaseName,
        
        [Parameter(Mandatory=$false, ParameterSetName="UNPW")]    
        [string]$MongoUsername,
        
        [Parameter(Mandatory=$true, ParameterSetName="UNPW")]    
        [string]$MongoPassword
    )
    
    if($Global:MONGO_SESSION_INFO.ImportedDlls){
        if((-not $PSBoundParameters["MongoDatabaseName"]) -and (-not $PSBoundParameters["MongoUsername"]) -and (-not $PSBoundParameters["MongoPassword"])){
            [PsMongo.PsMongoContext]::new($MongoServerHostname)
        }
        if($PSBoundParameters["MongoDatabaseName"]){
            [PsMongo.PsMongoContext]::new($MongoServerHostname, $MongoDatabaseName)  
        }
        if(-not ($PSBoundParameters["MongoDatabaseName"]) -and ($PSBoundParameters["MongoUsername"]) -and ($PSBoundParameters["MongoPassword"])){
            [PsMongo.PsMongoContext]::new($MongoServerHostname, $MongoUsername, $MongoPassword)
        }
        if(($PSBoundParameters["MongoDatabaseName"]) -and ($PSBoundParameters["MongoUsername"]) -and ($PSBoundParameters["MongoPassword"])){
            [PsMongo.PsMongoContext]::new($MongoServerHostname, $MongoDatabaseName, $MongoUsername, $MongoPassword)
        }                  
    } else {
        Write-Host "Unable to connect to Mongo Server '$MongoServerHostname' on database '$MongoDatabaseName' because the required libraries are not loaded!" -f Red
    }

}

function Get-AllDocumentsFromMongoCollection
{

    param(
        [Parameter(Mandatory=$true)]
        [PsMongo.PsMongoContext]$MongoConnection,
        
        [Parameter(Mandatory=$true)]
        [string]$CollectionName
    )
    
    if($Global:MONGO_SESSION_INFO.ImportedDlls){
        try {
            $AllJsonDocuments = @()
            $AllBsonDocuments = $MongoConnection.GetAllDocumentsFromCollection($CollectionName)
            if($AllBsonDocuments.Count -gt 0){
                foreach($bd in $AllBsonDocuments){
                    $AllJsonDocuments += ConvertFrom-BsonToJson -BsonDocument $bd
                }
                # return all documents as json
                $AllJsonDocuments
            } else {
                Write-Host "No documents found in collection '$($CollectionName.ToUpper())'" -f Yellow
            }
        } catch {
            Write-Host "[Connected Mongo Database may be null, are you connected to a database?]::Unable to get documents from collection '$CollectionName'!" -f Red
        }
    } else {
        Write-Host "Unable to get documents from collection '$CollectionName' because the required libraries are not loaded!" -f Red
    }

}

function ConvertFrom-BsonToJson
{
    
    param(
        [Parameter(Mandatory=$true)]
        [MongoDB.Bson.BsonDocument]$BsonDocument              
    )
    
    if($Global:MONGO_SESSION_INFO.ImportedDlls){
        [PsMongo.MongoConverter]::BSONtoJSON($BsonDocument) | ConvertFrom-Json    
    } else {
        Write-Host "Unable to convert Bson Document to JSON, because the required libraries are not loaded!" -f Red
    }            
}

function Add-JsonDocumentIntoMongoCollection
{

    param(
        [Parameter(Mandatory=$true)]
        [PsMongo.PsMongoContext]$MongoConnection,
        
        [Parameter(Mandatory=$true)]
        [string]$CollectionName,
    
        [Parameter(Mandatory=$true)]
        [string]$JsonData
    )
    
    if($Global:MONGO_SESSION_INFO.ImportedDlls){     
        try {     
            $stringToJson = $null
            $stringToJson = $JsonData | ConvertFrom-Json -ErrorAction SilentlyContinue -WarningAction SilentlyContinue
        } catch { 
            Write-Host "[Add-JsonDocumentIntoMongoCollection:param:JsonData::_value_]Not valid JSON data" -f Red 
        } 
        try { 
            $BsonData   = [PsMongo.MongoConverter]::JSONtoBSON($JsonData)
            $Collection = $MongoConnection.GetMongoCollection($CollectionName)
            $Collection.InsertOne($BsonData)
        } catch {
            Write-Host "Something went wrong saving document to database '$($MongoConnection.MongoDatabaseName)' in collection ''! Full Error: $($_)" -f Red
        }        
    } else {
        Write-Host "Either unable to import JSON data from '$JsonData' or save data to Mongo Collection '$CollectionName', because the required libraries are not loaded!" -f Red
    }
}   

function Remove-JsonDocumentFromMongoCollection
{

    param(
        [Parameter(Mandatory=$true)]
        [PsMongo.PsMongoContext]$MongoConnection,
        
        [Parameter(Mandatory=$true)]
        [string]$CollectionName,
    
        [Parameter(Mandatory=$true)]
        [string]$JsonDocument
    )
    
    if($Global:MONGO_SESSION_INFO.ImportedDlls){
    
        $MongoConnection.RemoveDocumentFromMongoCollection($JsonDocument, $CollectionName)
    } else {
        Write-Host "Unable to remove JSON Document from MongoDB because the required libraries are not loaded!" -f Red
    }

}

function New-MongoDatabase
{

    param(
        [Parameter(Mandatory=$true)]
        [PsMongo.PsMongoContext]$MongoConnection,
        
        [Parameter(Mandatory=$true)]
        [string]$NewDatabaseName,
        
        [Parameter(Mandatory=$true)]
        [string]$NewCollectionName # empty databases arent allowed in Mongo so we have to either insert a collection, or insert data
    )

    if($Global:MONGO_SESSION_INFO.ImportedDlls){
        $MongoConnection.CreateNewDatabase($NewDatabaseName, $NewCollectionName)
    } else {
        Write-Host "Unable to create new database because the required libraries are not loaded!" -f Red
    }

}

function Bind-ToMongoDatabase
{

    param(
        [Parameter(Mandatory=$true)]
        [PsMongo.PsMongoContext]$MongoConnection,
        
        [Parameter(Mandatory=$true)]
        [string]$DatabaseName
    )
    
    if($Global:MONGO_SESSION_INFO.ImportedDlls){
        $MongoConnection.ConnectToMongoDatabase($DatabaseName)
    } else {
        Write-Host "Unable to connect to database because the required libraries are not loaded!" -f Red
    }    
    
}

function Remove-MongoDatabase
{

    param(
        [Parameter(Mandatory=$true)]
        [PsMongo.PsMongoContext]$MongoConnection,
        
        [Parameter(Mandatory=$true)]
        [string]$DatabaseName,
        
        [Parameter(Mandatory=$false)]
        [switch]$ForceDatabaseDrop
    )
    
    if($Global:MONGO_SESSION_INFO.ImportedDlls){
        if(-not ($PSBoundParameters["ForceDatabaseDrop"])){
            $ShouldContinue = Confirm-Selection -Confirm DatabaseDrop
            if($ShouldContinue){
                $MongoConnection.RemoveDatabase($DatabaseName)
            }
        } else {
            $MongoConnection.RemoveDatabase($DatabaseName)
        }
    } else {
        Write-Host "Unable to remove database because the required libraries are not loaded!" -f Red
    }

}

function New-MongoCollection
{

    param(
        [Parameter(Mandatory=$true)]
        [PsMongo.PsMongoContext]$MongoConnection,
        
        [Parameter(Mandatory=$true)]
        [string]$NewCollectionName    
    )
    
    if($Global:MONGO_SESSION_INFO.ImportedDlls){
        if($MongoConnection.MongoDatabaseName -eq $null){
            Write-Host "You are not currently connected to any databases! Please connect to a database in order to create a collection!" -f Red
        } else {
            # create new collection in database we are currently connected to
            $MongoConnection.CreateNewCollection($NewCollectionName)
        }
    } else {
        Write-Host "Unable to create new collection because the required libraries are not loaded!" -f Red
    }
    
}

function Remove-MongoCollection
{

    param(
        [Parameter(Mandatory=$true)]
        [PsMongo.PsMongoContext]$MongoConnection,
        
        [Parameter(Mandatory=$true)]
        [string]$CollectionName,
        
        [Parameter(Mandatory=$false)]
        [switch]$ForceCollectionDrop
    )
    
    if($Global:MONGO_SESSION_INFO.ImportedDlls){
        if($MongoConnection.MongoDatabaseName -eq $null){
            Write-Host "You are not currently connected to any databases! Please connect to a database in order to create a collection!" -f Red
        } else {
            if(-not ($PSBoundParameters["ForceCollectionDrop"])){
                $ShouldContinue = Confirm-Selection -Confirm CollectionDrop
                if($ShouldContinue){
                    $MongoConnection.RemoveCollection($CollectionName)
                }
            } else {
                $MongoConnection.RemoveCollection($CollectionName)
            }
        }
    } else {
        Write-Host "Unable to remove collection because the required libraries are not loaded!" -f Red
    }

}

function Confirm-Selection
{

    param(
        [Parameter(Mandatory=$false)]
        [ValidateSet("DatabaseDrop", "CollectionDrop")]
        [string]$Confirm
    )

    switch($Confirm){
        "DatabaseDrop" {  
            Write-Host "`r`n`r`nIf the 'ForceDatabaseDrop' switch is not used you have to confirm that you would like to drop a database...`r`n" -f Yellow
        }
        "CollectionDrop" {  
            Write-Host "`r`n`r`nIf the 'ForceCollectionDrop' switch is not used you have to confirm that you would like to drop a collection...`r`n" -f Yellow
        }
    }    
    Write-Host "Would you like to continue?`r`n    1. Yes`r`n    2. No" -f Red    
    [string]$Answer = Read-Host "(1=Continue|2=Cancel)"
    if(($Answer -ne "1") -and ($Answer -ne "2")){
        Write-Host "Please make a valid selection!" -f Red
        Pause
        Confirm-Selection
    } else {
        switch($Answer){
            "1" { return $true }
            "2" { return $false }
        }
    }
}





# has to be the last thing in module
try {
    Export-ModuleMember *
} catch {
    # do nothing, only here to suppress ISE errors
}
