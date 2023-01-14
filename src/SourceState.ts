import * as Model from './Model.js'

export function newSourceState(): Model.SourceState {
    return {
        currentCommitSha: null,
        tags: {}
    }
}

export function setTagValue(source: Model.SourceState, name: string, value: any) {
    source.tags[name] = value

    // compatibility upgrade hack
    if (name == "readonly")
        setReadOnly(source, isReadOnly(source))
}

export function isReadOnly(sourceState: Model.SourceState) {
    if (!sourceState)
        return false
    if (sourceState.tags && ("readonly" in sourceState.tags))
        return !!sourceState.tags["readonly"]
    return !!sourceState.readOnly
}

export function setReadOnly(sourceState: Model.SourceState, value: boolean) {
    if (!sourceState.tags) {
        sourceState.tags = {}
    }

    sourceState.tags["readonly"] = value

    sourceState.readOnly = value
}

export function isIndexed(sourceState: Model.SourceState) {
    if (!sourceState)
        return false
    return sourceState.tags && !!sourceState.tags["indexed"]
}

export function setIndexed(sourceState: Model.SourceState, value: boolean) {
    if (!sourceState.tags) {
        sourceState.tags = {}
    }

    sourceState.tags["indexed"] = value
}