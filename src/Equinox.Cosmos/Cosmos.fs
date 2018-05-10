﻿namespace Equinox.Cosmos

open Equinox
open Equinox.Store
open FSharp.Control
open Microsoft.Azure.Documents
open Newtonsoft.Json
open Serilog
open System

[<AutoOpen>]
module ArraySegmentExtensions =

    type System.Text.Encoding with
        member x.GetString(data:ArraySegment<byte>) = x.GetString(data.Array, data.Offset, data.Count)

type ByteArrayConverter() =
    inherit JsonConverter()

    override this.ReadJson(reader, _, _, serializer) =
        let s = serializer.Deserialize(reader, typeof<string>) :?> string
        if s = null
        then
            let arr: byte[] = [||]
            // Why Array.Empty :> obj doesn't work here....
            arr :> obj
        else
            System.Text.Encoding.UTF8.GetBytes(s) :> obj

    override this.CanConvert(objectType) =
        typeof<byte[]>.Equals(objectType)

    override this.WriteJson(writer, value, serializer) =
        let array = value :?> byte[]
        if Array.length array = 0
        then
            serializer.Serialize(writer, null)
        else
            serializer.Serialize(writer, System.Text.Encoding.UTF8.GetString(array))

type SN = int64

[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module SN =

    /// The first sequence number.
    let [<Literal>] zero : SN = 0L

    /// The last sequence number
    let [<Literal>] last : SN = -1L

    /// Computes the next sequence number.
    let inline next (sn : SN) : SN = sn + 1L

    /// Computes the previous sequence number
    let inline prev (sn: SN): SN = sn - 1L

    /// Compares two sequence numbers.
    let inline compare (sn1 : SN) (sn2 : SN) : int = Operators.compare sn1 sn2

type StreamId = string

[<NoEquality; NoComparison>]
/// Event data.
type EventData = {
    eventType : string
    data : byte[]
    metadata : byte[] option }
    with

    static member create (eventType: string, data: byte[], ?metadata: byte[]) =
        {
            eventType = eventType
            data = data
            metadata =
                match metadata with
                | None -> None
                | Some md -> md |> Some }

/// Operations on event data.
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
[<RequireQualifiedAccess>]
module EventData =

    let eventType (ed:EventData) = ed.eventType
    let data (ed:EventData) = ed.data
    let metadata (ed:EventData) = ed.metadata

[<NoEquality; NoComparison>]
type EquinoxEvent = {
    id : string
    s : StreamId
    k : string
    ts : DateTimeOffset
    sn : SN
    et : string

    [<JsonConverter(typeof<ByteArrayConverter>)>]
    d : byte[]

    [<JsonConverter(typeof<ByteArrayConverter>)>]
    md : byte[] }

type Connection = IDocumentClient * Uri

[<RequireQualifiedAccess>]
type Direction = Forward | Backward with
    override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

module Log =
    [<NoEquality; NoComparison>]
    type Measurement = { stream: string; interval: StopwatchInterval; bytes: int; count: int; ru: int }
    [<NoEquality; NoComparison>]
    type Event =
        | WriteSuccess of Measurement
        | WriteConflict of Measurement
        | Slice of Direction * Measurement
        | Batch of Direction * slices: int * Measurement
    let prop name value (log : ILogger) = log.ForContext(name, value)
    open Serilog.Events
    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let event (value : Event) (log : ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty("eqxEvt", ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })
    let withLoggedRetries<'t> retryPolicy (contextLabel : string) (f : ILogger -> Async<'t>) log: Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy withLoggingContextWrapping
    let (|BlobLen|) = function null -> 0 | (x : byte[]) -> x.Length

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type EqxSyncResult = Written of SN * float | Conflict of float

module private Write =

    let private eventDataToEquinoxEvent (streamId:StreamId) (sequenceNumber:SN) (ed: EventData) =
        {
            EquinoxEvent.et = ed.eventType
            EquinoxEvent.id = (sprintf "%s-e-%d" streamId sequenceNumber)
            EquinoxEvent.s = streamId
            EquinoxEvent.k = streamId
            EquinoxEvent.d = ed.data
            EquinoxEvent.md =
                match ed.metadata with
                | Some x -> x
                | None -> [||]
            EquinoxEvent.sn = sequenceNumber
            EquinoxEvent.ts = DateTimeOffset.UtcNow
        }

    let [<Literal>] private multiDocInsert = "AtomicMultiDocInsert"

    let inline private sprocUri (sprocName : string) (collectionUri : Uri) =
        (collectionUri.ToString()) + "/sprocs/" + sprocName // TODO: do this elegantly

    /// Appends the single EventData using the sdk CreateDocumentAsync
    let private appendSingleEvent ((client, collectionUri) : Connection) streamId sequenceNumber eventData : Async<SN * float> =
        async {
            let sequenceNumber = (SN.next sequenceNumber)

            let equinoxEvent =
                eventData
                |> eventDataToEquinoxEvent streamId sequenceNumber

            let! res =
                client.CreateDocumentAsync (collectionUri, equinoxEvent)
                |> Async.AwaitTaskCorrect

            return (sequenceNumber, res.RequestCharge)
        }

    /// Appends the given EventData batch using the atomic stored procedure
    // This requires store procuedure in CosmosDB, is there other ways to do this?
    let private appendEventBatch ((client, collectionUri) : Connection) streamId sequenceNumber eventsData : Async<SN * float> =
        async {
            let sequenceNumber = (SN.next sequenceNumber)
            let res, sn =
                eventsData
                |> Seq.mapFold (fun sn ed -> (eventDataToEquinoxEvent streamId sn ed) |> JsonConvert.SerializeObject, SN.next sn) sequenceNumber

            let requestOptions =
                Client.RequestOptions(PartitionKey = PartitionKey(streamId))

            let! res =
                client.ExecuteStoredProcedureAsync<bool>(collectionUri |> sprocUri multiDocInsert, requestOptions, res:> obj)
                |> Async.AwaitTaskCorrect

            return (sn - 1L), res.RequestCharge
        }

    let private append connection streamId sequenceNumber (eventsData: EventData seq) =
        match Seq.length eventsData with
        | l when l = 0 -> invalidArg "eventsData" "must be non-empty"
        | l when l = 1 ->
            eventsData
            |> Seq.head
            |> appendSingleEvent connection streamId sequenceNumber
        | _ -> appendEventBatch connection streamId sequenceNumber eventsData

    let private writeEventsAsync (log : ILogger) (conn : Connection) (streamName : string) (version : int64) (events : EventData[])
        : Async<EqxSyncResult> = async {
        try
            let! wr = append conn streamName version events
            return EqxSyncResult.Written wr
        with ex ->
            // change this for store procudure
            match ex with
            | :? DocumentClientException as dce ->
                // Improve this?
                if dce.Message.Contains "already"
                then
                    log.Information(ex, "Ges TrySync WrongExpectedVersionException")
                    return EqxSyncResult.Conflict dce.RequestCharge
                else
                    return raise dce
            | e -> return raise e }

    let eventDataBytes events =
        let eventDataLen (x : EventData) =
            let data = x.data
            let metaData =
                match x.metadata with
                | None -> [||]
                | Some x -> x
            match data, metaData with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
        events |> Array.sumBy eventDataLen

    let private writeEventsLogged (conn : Connection) (streamName : string) (version : int64) (events : EventData[]) (log : ILogger)
        : Async<EqxSyncResult> = async {
        let bytes, count = eventDataBytes events, events.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog = log |> Log.prop "stream" streamName |> Log.prop "expectedVersion" version |> Log.prop "count" count
        let! t, result = writeEventsAsync writeLog conn streamName version events |> Stopwatch.Time
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count; ru = 0}
        let resultLog, evt, (ru: float) =
            match result, reqMetric with
            | EqxSyncResult.Conflict ru, m -> log, Log.WriteConflict { m with ru = Convert.ToInt32(ru) }, ru
            | EqxSyncResult.Written (x, ru), m ->
                log |> Log.prop "nextExpectedVersion" x |> Log.prop "ru" ru,
                Log.WriteSuccess { m with ru = Convert.ToInt32(ru) },
                ru
        // TODO drop expectedVersion when consumption no longer requires that literal; ditto stream when literal formatting no longer required
        (resultLog |> Log.event evt).Information("Eqx{action:l} stream={stream} count={count} expectedVersion={expectedVersion} conflict={conflict}, RequestCharge={ru}",
            "Write", streamName, events.Length, version, (match evt with Log.WriteConflict _ -> true | _ -> false), ru)
        return result }

    let writeEvents (log : ILogger) retryPolicy (conn : Connection) (streamName : string) (version : int64) (events : EventData[])
        : Async<EqxSyncResult> =
        let call = writeEventsLogged conn streamName version events
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

module private Read =
    open Microsoft.Azure.Documents.Linq
    open System.Linq

    let private getQuery ((client, collectionUri): Connection) streamId (direction: Direction) batchSize sequenceNumber =

        let sequenceNumber =
            match direction, sequenceNumber with
            | Direction.Backward, SN.last -> Int64.MaxValue
            | _ -> sequenceNumber

        let feedOptions = new Client.FeedOptions()
        feedOptions.PartitionKey <- PartitionKey(streamId)
        feedOptions.MaxItemCount <- Nullable(batchSize)
        let sql =
            match direction with
            | Direction.Backward ->
                let query = """
                SELECT * FROM c
                WHERE c.s = @streamId
                AND c.sn <= @sequenceNumber
                ORDER BY c.sn DESC"""
                SqlQuerySpec query
            | Direction.Forward ->
                let query = """
                SELECT * FROM c
                WHERE c.s = @streamId
                AND c.sn >= @sequenceNumber
                ORDER BY c.sn ASC """
                SqlQuerySpec query
        sql.Parameters <- SqlParameterCollection
            [|
            SqlParameter("@streamId", streamId)
            SqlParameter("@sequenceNumber", sequenceNumber)
            |]
        client.CreateDocumentQuery<EquinoxEvent>(collectionUri, sql, feedOptions).AsDocumentQuery()

    let (|EquinoxEventLen|) (x : EquinoxEvent) = match x.d, x.md with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes

    let private lastSequenceNumber (xs:EquinoxEvent seq) : SN =
        match xs |> Seq.tryLast with
        | None -> SN.last
        | Some last -> last.sn

    let private queryExecution (query: IDocumentQuery<'T>) =
        async {
            let! res = query.ExecuteNextAsync<'T>() |> Async.AwaitTaskCorrect
            return res.ToArray(), res.RequestCharge }

    let private loggedQueryExecution streamName direction batchSize startPos (query: IDocumentQuery<EquinoxEvent>) (log: ILogger)
        : Async<EquinoxEvent[] * float> = async {
        let! t, (slice, ru) = queryExecution query |> Stopwatch.Time
        let bytes, count = slice |> Array.sumBy (|EquinoxEventLen|), slice.Length
        let reqMetric : Log.Measurement ={ stream = streamName; interval = t; bytes = bytes; count = count; ru = Convert.ToInt32(ru) }
        let evt = Log.Slice (direction, reqMetric)
        (log |> Log.prop "startPos" startPos |> Log.prop "bytes" bytes |> Log.prop "ru" ru |> Log.event evt).Information(
            // TODO drop sliceLength, totalPayloadSize when consumption no longer requires that literal; ditto stream when literal formatting no longer required
            "Eqx{action:l} stream={stream} count={count} version={version} sliceLength={sliceLength} totalPayloadSize={totalPayloadSize} RequestCharge={ru}",
            "Read", streamName, count, (lastSequenceNumber slice), batchSize, bytes, ru)
        return slice, ru }

    let private readBatches (log : ILogger) (readSlice: IDocumentQuery<EquinoxEvent> -> ILogger -> Async<EquinoxEvent[] * float>) (maxPermittedBatchReads: int option) (query: IDocumentQuery<EquinoxEvent>)
        : AsyncSeq<EquinoxEvent[] * float> =
        let rec loop batchCount : AsyncSeq<EquinoxEvent[] * float> = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! slice = readSlice query batchLog
            yield slice
            if query.HasMoreResults then
                yield! loop (batchCount + 1)}
            //| x -> raise <| System.ArgumentOutOfRangeException("SliceReadStatus", x, "Unknown result value") }
        loop 0

    let equinoxEventBytes events = events |> Array.sumBy (|EquinoxEventLen|)

    let logBatchRead direction streamName t events batchSize version (ru: float) (log : ILogger) =
        let bytes, count = equinoxEventBytes events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count; ru = Convert.ToInt32(ru) }
        let batches = (events.Length - 1)/batchSize + 1
        let action = match direction with Direction.Forward -> "LoadF" | Direction.Backward -> "LoadB"
        let evt = Log.Event.Batch (direction, batches, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Eqx{action:l} stream={stream} count={count}/{batches} version={version} RequestCharge={ru}",
            action, streamName, count, batches, version, ru)

    let loadForwardsFrom (log : ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName startPosition
        : Async<int64 * EquinoxEvent[]> = async {
        let mutable ru = 0.0
        let mergeBatches (batches: AsyncSeq<EquinoxEvent[] * float>) = async {
            let! (events : EquinoxEvent[]) =
                batches
                |> AsyncSeq.map (fun (events, r) -> ru <- ru + r; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            return events, ru }
        let query = getQuery conn streamName Direction.Forward batchSize startPosition
        let call q = loggedQueryExecution streamName Direction.Forward batchSize startPosition q
        let retryingLoggingReadSlice q = Log.withLoggedRetries retryPolicy "readAttempt" (call q)
        let direction = Direction.Forward
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "direction" direction |> Log.prop "stream" streamName
        let batches : AsyncSeq<EquinoxEvent[] * float> = readBatches log retryingLoggingReadSlice maxPermittedBatchReads query
        let! t, (events, ru) = mergeBatches batches |> Stopwatch.Time
        (query :> IDisposable).Dispose()
        let version = lastSequenceNumber events
        log |> logBatchRead direction streamName t events batchSize version ru
        return version, events }

    let partitionPayloadFrom firstUsedEventNumber : EquinoxEvent[] -> int * int =
        let acc (tu,tr) ((EquinoxEventLen bytes) as y) = if y.sn < firstUsedEventNumber then tu, tr + bytes else tu + bytes, tr
        Array.fold acc (0,0)
    let loadBackwardsUntilCompactionOrStart (log : ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName isCompactionEvent
        : Async<int64 * EquinoxEvent[]> = async {
        let mergeFromCompactionPointOrStartFromBackwardsStream (log : ILogger) (batchesBackward : AsyncSeq<EquinoxEvent[] * float>)
            : Async<EquinoxEvent[] * float> = async {
            let lastBatch = ref None
            let mutable ru = 0.0
            let! tempBackward =
                batchesBackward
                |> AsyncSeq.map (fun (events, r) -> lastBatch := Some events; ru <- ru + r; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (fun x ->
                    if not (isCompactionEvent x) then true // continue the search
                    else
                        match !lastBatch with
                        | None -> log.Information("EqxStop stream={stream} at={eventNumber}", streamName, x.sn)
                        | Some batch ->
                            let used, residual = batch |> partitionPayloadFrom x.sn
                            log.Information("EqxStop stream={stream} at={eventNumber} used={used} residual={residual}", streamName, x.sn, used, residual)
                        false)
                |> AsyncSeq.toArrayAsync
            let eventsForward = Array.Reverse(tempBackward); tempBackward // sic - relatively cheap, in-place reverse of something we own
            return eventsForward, ru }
        let query = getQuery conn streamName Direction.Backward batchSize SN.last
        let call q = loggedQueryExecution streamName Direction.Backward batchSize SN.last q
        let retryingLoggingReadSlice q = Log.withLoggedRetries retryPolicy "readAttempt" (call q)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" streamName
        let direction = Direction.Backward
        let readlog = log |> Log.prop "direction" direction
        let batchesBackward : AsyncSeq<EquinoxEvent[] * float> = readBatches readlog retryingLoggingReadSlice maxPermittedBatchReads query
        let! t, (events, ru) = mergeFromCompactionPointOrStartFromBackwardsStream log batchesBackward |> Stopwatch.Time
        (query :> IDisposable).Dispose()
        let version = lastSequenceNumber events
        log |> logBatchRead direction streamName t events batchSize version ru
        return version, events }

module UnionEncoderAdapters =
    let private encodedEventOfResolvedEvent (x : EquinoxEvent) : UnionCodec.EncodedUnion<byte[]> =
        { caseName = x.et; payload = x.d }
    let private eventDataOfEncodedEvent (x : UnionCodec.EncodedUnion<byte[]>) =
        EventData.create(x.caseName, x.payload, [||])
    let encodeEvents (codec : UnionCodec.IUnionEncoder<'event, byte[]>) (xs : 'event seq) : EventData[] =
        xs |> Seq.map (codec.Encode >> eventDataOfEncodedEvent) |> Seq.toArray
    let decodeKnownEvents (codec : UnionCodec.IUnionEncoder<'event, byte[]>) (xs : EquinoxEvent[]) : 'event seq =
        xs |> Seq.map encodedEventOfResolvedEvent |> Seq.choose codec.TryDecode

type Token = { streamVersion: int64; compactionEventNumber: int64 option }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Token =
    let private create compactionEventNumber batchCapacityLimit streamVersion : Storage.StreamToken =
        { value = box { streamVersion = streamVersion; compactionEventNumber = compactionEventNumber }; batchCapacityLimit = batchCapacityLimit }
    /// No batching / compaction; we only need to retain the StreamVersion
    let ofNonCompacting streamVersion : Storage.StreamToken =
        create None None streamVersion
    // headroom before compaction is necessary given the stated knowledge of the last (if known) `compactionEventNumberOption`
    let private batchCapacityLimit compactedEventNumberOption unstoredEventsPending (batchSize : int) (streamVersion : int64) : int =
        match compactedEventNumberOption with
        | Some (compactionEventNumber : int64) -> (batchSize - unstoredEventsPending) - int (streamVersion - compactionEventNumber + 1L) |> max 0
        | None -> (batchSize - unstoredEventsPending) - (int streamVersion + 1) - 1 |> max 0
    let (*private*) ofCompactionEventNumber compactedEventNumberOption unstoredEventsPending batchSize streamVersion : Storage.StreamToken =
        let batchCapacityLimit = batchCapacityLimit compactedEventNumberOption unstoredEventsPending batchSize streamVersion
        create compactedEventNumberOption (Some batchCapacityLimit) streamVersion
    /// Assume we have not seen any compaction events; use the batchSize and version to infer headroom
    let ofUncompactedVersion batchSize streamVersion : Storage.StreamToken =
        ofCompactionEventNumber None 0 batchSize streamVersion
    /// Use previousToken plus the data we are adding and the position we are adding it to infer a headroom
    let ofPreviousTokenAndEventsLength (previousToken : Storage.StreamToken) eventsLength batchSize streamVersion : Storage.StreamToken =
        let compactedEventNumber = (unbox previousToken.value).compactionEventNumber
        ofCompactionEventNumber compactedEventNumber eventsLength batchSize streamVersion
    /// Use an event just read from the stream to infer headroom
    let ofCompactionResolvedEventAndVersion (compactionEvent: EquinoxEvent) batchSize streamVersion : Storage.StreamToken =
        ofCompactionEventNumber (Some compactionEvent.sn) 0 batchSize streamVersion
    /// Use an event we are about to write to the stream to infer headroom
    let ofPreviousStreamVersionAndCompactionEventDataIndex prevStreamVersion compactionEventDataIndex eventsLength batchSize streamVersion' : Storage.StreamToken =
        ofCompactionEventNumber (Some (prevStreamVersion + 1L + int64 compactionEventDataIndex)) eventsLength batchSize streamVersion'
    let private unpackGesStreamVersion (x : Storage.StreamToken) = let x : Token = unbox x.value in x.streamVersion
    let supersedes current x =
        let currentVersion, newVersion = unpackGesStreamVersion current, unpackGesStreamVersion x
        newVersion > currentVersion

type EqxConnection(connection : IDocumentClient, ?readRetryPolicy, ?writeRetryPolicy) =
    member __.Connection = connection, Client.UriFactory.CreateDocumentCollectionUri("test","test")
    member __.ReadRetryPolicy = readRetryPolicy
    member __.WriteRetryPolicy = writeRetryPolicy

type EqxBatchingPolicy(getMaxBatchSize : unit -> int, ?batchCountLimit) =
    new (maxBatchSize) = EqxBatchingPolicy(fun () -> maxBatchSize)
    member __.BatchSize = getMaxBatchSize()
    member __.MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of Storage.StreamToken | Conflict

type EqxGateway(conn : EqxConnection, batching : EqxBatchingPolicy) =
    let isResolvedEventEventType predicate (x:EquinoxEvent) = predicate x.et
    let tryIsResolvedEventEventType predicateOption = predicateOption |> Option.map isResolvedEventEventType
    member __.LoadBatched streamName log isCompactionEventType: Async<Storage.StreamToken * EquinoxEvent[]> = async {
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Connection batching.BatchSize batching.MaxBatches streamName 0L
        match tryIsResolvedEventEventType isCompactionEventType with
        | None -> return Token.ofNonCompacting version, events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofUncompactedVersion batching.BatchSize version, events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize version, events }
    member __.LoadBackwardsStoppingAtCompactionEvent streamName log isCompactionEventType: Async<Storage.StreamToken * EquinoxEvent[]> = async {
        let isCompactionEvent = isResolvedEventEventType isCompactionEventType
        let! version, events =
            Read.loadBackwardsUntilCompactionOrStart log conn.ReadRetryPolicy conn.Connection batching.BatchSize batching.MaxBatches streamName isCompactionEvent
        match Array.tryHead events |> Option.filter isCompactionEvent with
        | None -> return Token.ofUncompactedVersion batching.BatchSize version, events
        | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize version, events }
    member __.LoadFromToken streamName log (token : Storage.StreamToken) isCompactionEventType
        : Async<Storage.StreamToken * EquinoxEvent[]> = async {
        let streamPosition = (unbox token.value).streamVersion
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Connection batching.BatchSize batching.MaxBatches streamName streamPosition
        match tryIsResolvedEventEventType isCompactionEventType with
        | None -> return Token.ofNonCompacting version, events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofPreviousTokenAndEventsLength token events.Length batching.BatchSize version, events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize version, events }
    member __.TrySync streamName log (token : Storage.StreamToken) (encodedEvents: EventData array) isCompactionEventType : Async<GatewaySyncResult> = async {
        let streamVersion = (unbox token.value).streamVersion
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.Connection streamName streamVersion encodedEvents
        match wr with
        | EqxSyncResult.Conflict _ -> return GatewaySyncResult.Conflict
        | EqxSyncResult.Written (wr, _) ->

        let version' = wr
        let token =
            match isCompactionEventType with
            | None -> Token.ofNonCompacting version'
            | Some isCompactionEvent ->
                let isEventDataEventType predicate (x:EventData) = predicate x.eventType
                match encodedEvents |> Array.tryFindIndexBack (isEventDataEventType isCompactionEvent) with
                | None -> Token.ofPreviousTokenAndEventsLength token encodedEvents.Length batching.BatchSize version'
                | Some compactionEventIndex ->
                    Token.ofPreviousStreamVersionAndCompactionEventDataIndex streamVersion compactionEventIndex encodedEvents.Length batching.BatchSize version'
        return GatewaySyncResult.Written token }

type EqxCategory<'event, 'state>(gateway : EqxGateway, codec : UnionCodec.IUnionEncoder<'event, byte[]>, ?compactionStrategy) =
    let loadAlgorithm load streamName initial log =
        let batched = load initial (gateway.LoadBatched streamName log None)
        let compacted predicate = load initial (gateway.LoadBackwardsStoppingAtCompactionEvent streamName log predicate)
        match compactionStrategy with
        | Some predicate -> compacted predicate
        | None -> batched
    let load (fold: 'state -> 'event seq -> 'state) initial f = async {
        let! token, events = f
        return token, fold initial (UnionEncoderAdapters.decodeKnownEvents codec events) }
    member __.Load (fold: 'state -> 'event seq -> 'state) (initial: 'state) (streamName : string) (log : ILogger) : Async<Storage.StreamToken * 'state> =
        loadAlgorithm (load fold) streamName initial log
    member __.LoadFromToken (fold: 'state -> 'event seq -> 'state) (state: 'state) (streamName : string) token (log : ILogger) : Async<Storage.StreamToken * 'state> =
        (load fold) state (gateway.LoadFromToken streamName log token compactionStrategy)
    member __.TrySync (fold: 'state -> 'event seq -> 'state) streamName (log : ILogger) (token, state) (events : 'event list) : Async<Storage.SyncResult<'state>> = async {
        let encodedEvents : EventData[] = UnionEncoderAdapters.encodeEvents codec (Seq.ofList events)
        let! syncRes = gateway.TrySync streamName log token encodedEvents compactionStrategy
        match syncRes with
        | GatewaySyncResult.Conflict ->         return Storage.SyncResult.Conflict  (load fold state (gateway.LoadFromToken streamName log token compactionStrategy))
        | GatewaySyncResult.Written token' ->   return Storage.SyncResult.Written   (token', fold state (Seq.ofList events)) }

module Caching =
    open System.Runtime.Caching
    [<AllowNullLiteral>]
    type CacheEntry<'state>(initialToken : Storage.StreamToken, initialState :'state) =
        let mutable currentToken, currentState = initialToken, initialState
        member __.UpdateIfNewer (other : CacheEntry<'state>) =
            lock __ <| fun () ->
                let otherToken, otherState = other.Value
                if otherToken |> Token.supersedes currentToken then
                    currentToken <- otherToken
                    currentState <- otherState
        member __.Value : Storage.StreamToken  * 'state =
            lock __ <| fun () ->
                currentToken, currentState

    type Cache(name, sizeMb : int) =
        let cache =
            let config = System.Collections.Specialized.NameValueCollection(1)
            config.Add("cacheMemoryLimitMegabytes", string sizeMb);
            new MemoryCache(name, config)
        member __.UpdateIfNewer (policy : CacheItemPolicy) (key : string) entry =
            match cache.AddOrGetExisting(key, box entry, policy) with
            | null -> ()
            | :? CacheEntry<'state> as existingEntry -> existingEntry.UpdateIfNewer entry
            | x -> failwithf "UpdateIfNewer Incompatible cache entry %A" x
        member __.TryGet (key : string) =
            match cache.Get key with
            | null -> None
            | :? CacheEntry<'state> as existingEntry -> Some existingEntry.Value
            | x -> failwithf "TryGet Incompatible cache entry %A" x

    /// Forwards all state changes in all streams of an ICategory to a `tee` function
    type CategoryTee<'event, 'state>(inner: ICategory<'event, 'state>, tee : string -> Storage.StreamToken * 'state -> unit) =
        let intercept streamName tokenAndState =
            tee streamName tokenAndState
            tokenAndState
        let interceptAsync load streamName = async {
            let! tokenAndState = load
            return intercept streamName tokenAndState }
        interface ICategory<'event, 'state> with
            member __.Load (streamName : string) (log : ILogger) : Async<Storage.StreamToken * 'state> =
                interceptAsync (inner.Load streamName log) streamName
            member __.TrySync streamName (log : ILogger) (token, state) (events : 'event list) : Async<Storage.SyncResult<'state>> = async {
                let! syncRes = inner.TrySync streamName log (token, state) events
                match syncRes with
                | Storage.SyncResult.Conflict resync ->             return Storage.SyncResult.Conflict (interceptAsync resync streamName)
                | Storage.SyncResult.Written (token', state') ->    return Storage.SyncResult.Written (token', state') }

    let applyCacheUpdatesWithSlidingExpiration
            (cache: Cache)
            (prefix: string)
            (slidingExpiration : TimeSpan)
            (category: ICategory<'event, 'state>)
            : ICategory<'event, 'state> =
        let policy = new CacheItemPolicy(SlidingExpiration = slidingExpiration)
        let addOrUpdateSlidingExpirationCacheEntry streamName = CacheEntry >> cache.UpdateIfNewer policy (prefix + streamName)
        CategoryTee<'event,'state>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

type EqxFolder<'event, 'state>(category : EqxCategory<'event, 'state>, fold: 'state -> 'event seq -> 'state, initial: 'state, ?readCache) =
    let loadAlgorithm streamName initial log =
        let batched = category.Load fold initial streamName log
        let cached token state = category.LoadFromToken fold state streamName token log
        match readCache with
        | None -> batched
        | Some (cache : Caching.Cache, prefix : string) ->
            match cache.TryGet(prefix + streamName) with
            | None -> batched
            | Some (token, state) -> cached token state
    interface ICategory<'event, 'state> with
        member __.Load (streamName : string) (log : ILogger) : Async<Storage.StreamToken * 'state> =
            loadAlgorithm streamName initial log
        member __.TrySync streamName (log : ILogger) (token, state) (events : 'event list) : Async<Storage.SyncResult<'state>> = async {
            let! syncRes = category.TrySync fold streamName log (token, state) events
            match syncRes with
            | Storage.SyncResult.Conflict resync ->         return Storage.SyncResult.Conflict resync
            | Storage.SyncResult.Written (token',state') -> return Storage.SyncResult.Written (token',state') }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CompactionStrategy =
    | EventType of string
    | Predicate of (string -> bool)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    | SlidingWindow of Caching.Cache * window: TimeSpan
    /// Prefix is used to distinguish multiple folds per stream
    | SlidingWindowPrefixed of Caching.Cache * window: TimeSpan * prefix: string

type EqxStreamBuilder<'event, 'state>(gateway, codec, fold, initial, ?compaction, ?caching) =
    member __.Create streamName : Equinox.IStream<'event, 'state> =
        let compactionPredicateOption =
            match compaction with
            | None -> None
            | Some (CompactionStrategy.Predicate predicate) -> Some predicate
            | Some (CompactionStrategy.EventType eventType) -> Some (fun x -> x = eventType)
        let eqxCategory = EqxCategory<'event, 'state>(gateway, codec, ?compactionStrategy = compactionPredicateOption)

        let readCacheOption =
            match caching with
            | None -> None
            | Some (CachingStrategy.SlidingWindow(cache, _)) -> Some(cache, null)
            | Some (CachingStrategy.SlidingWindowPrefixed(cache, _, prefix)) -> Some(cache, prefix)
        let folder = EqxFolder<'event, 'state>(eqxCategory, fold, initial, ?readCache = readCacheOption)

        let category : ICategory<_,_> =
            match caching with
            | None -> folder :> _
            | Some (CachingStrategy.SlidingWindow(cache, window)) ->
                Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder
            | Some (CachingStrategy.SlidingWindowPrefixed(cache, window, prefix)) ->
                Caching.applyCacheUpdatesWithSlidingExpiration cache prefix window folder

        Equinox.Stream.create category streamName

[<RequireQualifiedAccess; NoComparison>]
type Discovery =
    | UriAndKey of Uri * string * string * string
    //| ConnecionString of string * string * string -> support this later

type EqxConnector
    (   requestTimeout: TimeSpan, maxRetryAttemptsOnThrottledRequests: int, maxRetryWaitTimeInSeconds: int,
        ?maxConnectionLimit) =
    let connPolicy =
        let cp = Client.ConnectionPolicy.Default
        cp.ConnectionMode <- Client.ConnectionMode.Direct
        cp.ConnectionProtocol <- Client.Protocol.Tcp
        cp.RetryOptions <-
            Client.RetryOptions(
                MaxRetryAttemptsOnThrottledRequests = maxRetryAttemptsOnThrottledRequests,
                MaxRetryWaitTimeInSeconds = maxRetryWaitTimeInSeconds)
        cp.RequestTimeout <- requestTimeout
        cp.MaxConnectionLimit <- defaultArg maxConnectionLimit 1000
        cp

    /// Yields an connection to DocDB configured and Connect()ed to DocDB collection per the requested `discovery` strategy
    member __.Connect (discovery  : Discovery) : Async<EqxConnection> = async {
        let client (uri: Uri) (key: string) (dbId: string) (collectionName: string) =
            let client = new Client.DocumentClient(uri, key, connPolicy, Nullable(ConsistencyLevel.Session))
            client

        let client =
            match discovery with
            | Discovery.UriAndKey (uri, key, dbName, collName) -> client uri key dbName collName

        do! client.OpenAsync() |> Async.AwaitTaskCorrect

        return EqxConnection(client :> IDocumentClient) }