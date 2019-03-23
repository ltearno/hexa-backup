const BASE_URL = "/"
//const HEXA_BACKUP_BASE_URL = "https://home.lteconsulting.fr"
const HEXA_BACKUP_BASE_URL = "https://192.168.0.2:5005"

const el = document.querySelector.bind(document)

let EXTENDED = localStorage.getItem('EXTENDED') === 'true'
el('#extended').checked = EXTENDED

let STREAM_RAW_VIDEO = localStorage.getItem('STREAM_RAW_VIDEO') === 'true'
el('#stream-raw-video').checked = STREAM_RAW_VIDEO

let SHOW_FULL_COMMIT_HISTORY = localStorage.getItem('SHOW_FULL_COMMIT_HISTORY') === 'true'
el('#show-full-commit-history').checked = SHOW_FULL_COMMIT_HISTORY

let SHOW_UNLIKED_ITEMS = localStorage.getItem('SHOW_UNLIKED_ITEMS') === 'true'
el('#show-unliked-items').checked = SHOW_UNLIKED_ITEMS

let currentClientId = null
let currentDirectoryDescriptorSha = null
let currentPictureIndex = -1
let currentAudioIndex = -1
let currentVideoIndex = -1
let filesPool = []
let filesShaLikeMetadata = {}
let imagesPool = []
let videosPool = []
let audioPool = []

let displayedDirectoryDescriptorSha = null
let displayedClientId = null
let displayedPictureIndex = null
let displayedExtended = EXTENDED
let displayedStreamRawVideo = STREAM_RAW_VIDEO
let displayedShowFullCommitHistory = SHOW_FULL_COMMIT_HISTORY
let displayedShowUnlikedItems = SHOW_UNLIKED_ITEMS

const wait = (ms) => {
    return new Promise(resolve => {
        setTimeout(() => {
            resolve()
        }, ms)
    })
}

const DATE_DISPLAY_OPTIONS = {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
}
const displayDate = date => (typeof date === 'number' ? new Date(date) : date).toLocaleString('fr', DATE_DISPLAY_OPTIONS)

let lastPushedHistoryState = null
const publishHistoryState = () => {
    let footprint = `${currentDirectoryDescriptorSha}#${currentClientId}`
    if (currentPictureIndex >= 0)
        footprint += `-${currentPictureIndex}`

    if (footprint != lastPushedHistoryState) {
        lastPushedHistoryState = footprint
        history.pushState({ currentDirectoryDescriptorSha, currentClientId, currentPictureIndex },
            `directory ${currentDirectoryDescriptorSha}`,
            `${BASE_URL}#${currentDirectoryDescriptorSha}${currentPictureIndex >= 0 ? `-${currentPictureIndex}` : ''}`)
    }
}

async function goDirectory(directoryDescriptorSha) {
    el("#menu").classList.add("hide-optional")

    currentPictureIndex = -1
    currentDirectoryDescriptorSha = directoryDescriptorSha
    publishHistoryState()

    await syncUi()
}

async function goRef(ref) {
    el("#menu").classList.add("hide-optional")

    currentClientId = ref
    currentDirectoryDescriptorSha = await getClientDefaultDirectoryDescriptorSha(ref)
    publishHistoryState()

    await syncUi()
}

async function goPicture(index) {
    currentPictureIndex = index
    publishHistoryState()

    await syncUi()
}

async function goPreviousPicture() {
    if (currentPictureIndex <= 0)
        return

    currentPictureIndex--
    publishHistoryState()

    await syncUi()
}

async function goNextPicture() {
    if (currentPictureIndex < 0 || !imagesPool || !imagesPool.length || currentPictureIndex == imagesPool.length - 1)
        return

    currentPictureIndex++
    publishHistoryState()

    await syncUi()
}

async function goNoPicture() {
    currentPictureIndex = -1

    publishHistoryState()

    await syncUi()
}

let loaders = []

let startLoading = (text) => {
    loaders.push(text)
    el('#status').innerHTML = loaders.join('<br>')
    return () => {
        loaders.splice(loaders.indexOf(text), 1)
        el('#status').innerHTML = loaders.join('<br>')
    }
}

async function showDirectory(directoryDescriptorSha) {
    el('#directories').innerHTML = ''
    el('#files').innerHTML = ''
    el('#images').innerHTML = ''
    el('#video-list').innerHTML = ''
    el('#audio-list').innerHTML = ''

    if (!directoryDescriptorSha)
        return

    let finishLoading = startLoading(`loading directory descriptor ${directoryDescriptorSha.substr(0, 7)}`)
    let directoryDescriptor = await fetchDirectoryDescriptor(directoryDescriptorSha)
    finishLoading()

    if (!directoryDescriptor || !directoryDescriptor.files) {
        el('#directories').innerHTML = `error fetching ${directoryDescriptorSha}`
        return
    }

    let files = directoryDescriptor.files

    finishLoading = startLoading(`listing directories`)
    let directoriesContent = files
        .filter(file => file.isDirectory)
        .sort((a, b) => {
            let sa = a.name.toLocaleLowerCase()
            let sb = b.name.toLocaleLowerCase()
            return sa.localeCompare(sb)
        })
        .map(file => EXTENDED ?
            `<div><span class='small'>${displayDate(file.lastWrite)} ${file.contentSha ? file.contentSha.substr(0, 7) : '-'}</span> <a href='#' onclick='event.preventDefault() || goDirectory("${file.contentSha}")'>${file.name}</a></div>` :
            `<div><a href='#' onclick='event.preventDefault() || goDirectory("${file.contentSha}")'>${file.name}</a></div>`)
    if (directoriesContent.length) {
        el('#directories').classList.remove('is-hidden')
        el('#directories').innerHTML = `<h2>${directoriesContent.length} Directories</h2><div id='directories-container'>${directoriesContent.join('')}</div>`
    }
    else {
        el('#directories').classList.add('is-hidden')
    }
    finishLoading()

    let images = []
    let videos = []
    let audios = []

    finishLoading = startLoading(`listing files`)
    filesPool = files
        .filter(file => !file.isDirectory)
        .sort((a, b) => {
            let sa = a.name.toLocaleLowerCase()
            let sb = b.name.toLocaleLowerCase()
            return sa.localeCompare(sb)
        })
        .map((file, index) => {
            return {
                sha: file.contentSha,
                mimeType: getMimeType(file.name),
                fileName: file.name,

                lastWrite: file.lastWrite,
                size: file.size
            }
        })

    await loadLikesFiles()

    filesPool.forEach(file => {
        if (file.mimeType.startsWith('image/'))
            images.push(file)

        if (file.mimeType.startsWith('video/'))
            videos.push(file)

        if (file.mimeType.startsWith('audio/'))
            audios.push(file)
    })

    await restartFilePool()
    finishLoading()

    await wait(1)

    imagesPool = images
    await restartImagesPool()

    currentVideoIndex = -1
    videosPool = videos
    await restartVideosPool()

    currentAudioIndex = -1
    audioPool = audios
    await restartAudioPool()
}

const maxImagesSeen = 100
const imagesStep = 100

let infiniteScrollerStop = null

function getMimeType(fileName) {
    let pos = fileName.lastIndexOf('.')
    if (pos >= 0) {
        let extension = fileName.substr(pos + 1).toLocaleLowerCase()
        if (extension in MimeTypes)
            return MimeTypes[extension]
    }

    return 'application/octet-stream'
}

async function restartFilePool() {
    let filesContent = filesPool.map((file, index) => {
        let mimeTypes = ['application/octet-stream']
        if (file.mimeType != 'application/octet-stream')
            mimeTypes.push(file.mimeType)

        let links = mimeTypes.map((mimeType, index) => `[<a href='${HEXA_BACKUP_BASE_URL}/sha/${file.sha}/content?type=${mimeType}${index == 0 ? `&fileName=${file.fileName}` : ''}' >${EXTENDED ? mimeType : (index == 0 ? 'dl' : (mimeType.indexOf('/') ? mimeType.substr(mimeType.indexOf('/') + 1) : mimeType))}</a>]`).join(' ')

        let imageHtml = EXTENDED ?
            `<span class='small'>${displayDate(file.lastWrite)} ${file.sha ? file.sha.substr(0, 7) : '-'}</span> ${file.fileName} <span class='small'>${file.size} ${links}</span>` :
            `${file.fileName} <span class='small'>${links} <a class='like' onclick='event.preventDefault() || toggleLikeFile(${index})'>like ♡</a></span>`

        return `<div>${imageHtml}</div>`
    })

    if (filesContent.length) {
        el('#files').classList.remove('is-hidden')
        el('#files').innerHTML = `<h2>${filesContent.length} Files</h2><div id='files-list'>${filesContent.join('')}</div>`
    }
    else {
        el('#files').classList.add('is-hidden')
    }

    await viewLikesFiles()
}

async function loadLikesFiles() {
    let allShas = [...new Set(filesPool.map(f => f.sha))]

    filesShaLikeMetadata = {}
    const BATCH_SIZE = 20
    let startIndex = 0
    while (startIndex < allShas.length) {
        let batchSize = Math.min(BATCH_SIZE, allShas.length - startIndex)
        let partResults = await (await fetch(`/metadata/likes-sha?q=${JSON.stringify({
            shaList: allShas.slice(startIndex, startIndex + batchSize)
        })}`)).json()

        startIndex += batchSize

        if (!partResults)
            continue

        Object.getOwnPropertyNames(partResults).forEach(sha => filesShaLikeMetadata[sha] = partResults[sha])
    }
}

async function viewLikesFiles() {
    for (let i in filesPool) {
        let file = filesPool[i]
        let metadata = filesShaLikeMetadata[file.sha]
        if (metadata && metadata.status)
            el('#files-list').children.item(i).classList.add('liked')
    }
}

async function viewLikedFiles(likes) {
    if (!likes)
        return

    filesPool = likes.map(like => {
        return {
            sha: like.sha,
            fileName: like.value.knownAs.fileName,
            mimeType: like.value.knownAs.mimeType,

            size: 0,
        }
    })

    await loadLikesFiles()

    await restartFilePool()
}



async function toggleLikeFile(index) {
    if (index < 0 || index >= filesPool.length)
        return

    let file = filesPool[index]

    let status = await toggleShaLike(file.sha, file.mimeType, file.fileName)

    if (status)
        el('#files-list').children.item(index).classList.add('liked')
    else
        el('#files-list').children.item(index).classList.remove('liked')
}




async function restartImagesPool() {
    if (infiniteScrollerStop) {
        infiniteScrollerStop()
        infiniteScrollerStop = null
    }

    if (imagesPool.length) {
        el('#images').innerHTML = ''
        infiniteScrollerStop = infiniteScroll(imagesPool,
            ({ sha, mimeType, fileName }, index) => `<div><img onclick='goPicture(${index})' src="${HEXA_BACKUP_BASE_URL}/sha/${sha}/plugins/image/thumbnail?type=${mimeType}"/></div>`,
            el('#images-container'),
            el('#images'))
    }
    else {
        el('#images').innerHTML = '<br/><i class="small">no picture in this folder</i>'
        // <a href='/sha/${sha}/content?type=${mimeType}'></a>
    }
}

async function showVideo(index) {
    if (index < 0 || index >= videosPool.length)
        return

    currentVideoIndex = index
    let { sha, mimeType, fileName } = videosPool[index]
    el('#video-player').setAttribute('src', STREAM_RAW_VIDEO ? `${HEXA_BACKUP_BASE_URL}/sha/${sha}/content?type=${mimeType}` : `${HEXA_BACKUP_BASE_URL}/sha/${sha}/plugins/video/small?type=${mimeType}`)
    el('#video-player').setAttribute('type', mimeType)
    el('#video-player').play()

    el(`#video-${index}`).style.fontWeight = 'bold'
}

async function showNextVideo() {
    showVideo(currentVideoIndex + 1)
}

async function toggleLikeVideo(index) {
    if (index < 0 || index >= videosPool.length)
        return

    let { sha, mimeType, fileName } = videosPool[index]
    let status = await toggleShaLike(sha, mimeType, fileName)

    if (status)
        el('#video-list').children.item(index).classList.add('liked')
    else
        el('#video-list').children.item(index).classList.remove('liked')
}

async function restartVideosPool() {
    let html = ''
    for (let i in videosPool) {
        html += `<div><a id='video-${i}' href='#' onclick='event.preventDefault(), showVideo(${i})'>${videosPool[i].fileName}</a> <a class='like' onclick='event.preventDefault() || toggleLikeVideo(${i})'>like ♡</a></div></div>`
    }
    el('#video-list').innerHTML = html

    if (videosPool.length)
        el('#videos-container').classList.remove('is-hidden')
    else
        el('#videos-container').classList.add('is-hidden')

    loadLikesVideo()
}

async function loadLikesVideo() {
    let i = 0
    while (i < videosPool.length) {
        let metadata = filesShaLikeMetadata[videosPool[i].sha] //await (await fetch(`/metadata/likes-sha/${videosPool[i].sha}`)).json()
        if (metadata && metadata.status)
            el('#video-list').children.item(i).classList.add('liked')
        i++
    }
}

async function viewLikedVideos(likes) {
    if (!likes)
        likes = []

    videosPool = likes.map(like => {
        return {
            sha: like.sha,
            fileName: like.value.knownAs.fileName,
            mimeType: like.value.knownAs.mimeType
        }
    })

    restartVideosPool()
}

async function listenAudio(index) {
    if (index < 0 || index >= audioPool.length)
        return

    currentAudioIndex = index
    let { sha, mimeType, fileName } = audioPool[index]
    el('#audio-player').setAttribute('src', `${HEXA_BACKUP_BASE_URL}/sha/${sha}/content?type=${mimeType}`)
    el('#audio-player').setAttribute('type', mimeType)
    el('#audio-player').play()

    el(`#audio-${index}`).style.fontWeight = 'bold'
}

async function toggleShaLike(sha, mimeType, fileName) {
    let metadata = filesShaLikeMetadata[sha]
    if (!metadata) {
        metadata = {
            dates: [Date.now()],
            status: false,
            knownAs: { mimeType, fileName, currentClientId, currentDirectoryDescriptorSha }
        }
    }

    metadata.status = !metadata.status
    if (!metadata.dates)
        metadata.dates = [Date.now()]
    else
        metadata.dates.push(Date.now())

    filesShaLikeMetadata[sha] = metadata

    // we are optimistic, we do not wait for server's response !
    const headers = new Headers()
    headers.set('Content-Type', 'application/json')
    fetch(`/metadata/likes-sha/${sha}`, {
        headers,
        method: 'post',
        body: JSON.stringify(metadata)
    })

    return metadata.status
}

async function toggleLikeAudio(index) {
    if (index < 0 || index >= audioPool.length)
        return

    let { sha, mimeType, fileName } = audioPool[index]
    let status = await toggleShaLike(sha, mimeType, fileName)

    if (status)
        el('#audio-list').children.item(index).classList.add('liked')
    else
        el('#audio-list').children.item(index).classList.remove('liked')
}

async function listenNext() {
    listenAudio(currentAudioIndex + 1)
}

async function listenToLiked(likes) {
    if (!likes)
        likes = []

    audioPool = likes.map(like => {
        return {
            sha: like.sha,
            fileName: like.value.knownAs.fileName,
            mimeType: like.value.knownAs.mimeType
        }
    })

    restartAudioPool()
}

async function restartAudioPool() {
    let html = ''
    for (let i in audioPool) {
        html += `<div><a id='audio-${i}' href='#' onclick='event.preventDefault() || listenAudio(${i})'>${audioPool[i].fileName}</a> <a class='like' onclick='event.preventDefault() || toggleLikeAudio(${i})'>like ♡</a></div>`
    }
    el('#audio-list').innerHTML = html

    if (audioPool.length)
        el('#audio-container').classList.remove('is-hidden')
    else
        el('#videos-container').classList.add('is-hidden')

    loadLikesAudio()
}

async function loadLikesAudio() {
    let i = 0
    // TODO fetch by batch
    // TODO fetch only once (files, videos, audios and images)
    while (i < audioPool.length) {
        let metadata = filesShaLikeMetadata[audioPool[i].sha]
        // TODO since we only use 'status', maybe an option to only fetch that
        if (metadata && metadata.status)
            el('#audio-list').children.item(i).classList.add('liked')
        i++
    }
}

async function showPicture(index) {
    el('#image-full-aligner').innerHTML = ''
    if (index < 0)
        return
    let { sha, mimeType, fileName } = imagesPool[index]
    el('#image-full-aligner').innerHTML += `<a style='width:100%;height:100%;display:flex;align-items:center;justify-content: center;' href='${HEXA_BACKUP_BASE_URL}/sha/${sha}/content?type=${mimeType}'><img class='image-full' src='/sha/${sha}/plugins/image/medium?type=${mimeType}'/></a>`
}

async function getClientDefaultDirectoryDescriptorSha(ref) {
    if (!ref)
        return null

    let clientState = await (await fetch(`${HEXA_BACKUP_BASE_URL}/refs/${ref}`)).json()
    if (!clientState)
        return null

    let commitSha = clientState.currentCommitSha
    if (commitSha == null)
        return null

    let finishLoading = startLoading(`loading commit ${commitSha.substr(0, 7)} for default directory descriptor`)

    let commit = await fetchCommit(commitSha)

    finishLoading()

    if (!commit)
        return null

    return commit.directoryDescriptorSha
}

async function showRef(ref) {
    el('#refName').innerText = ref || ''
    el('#commitHistory').innerHTML = ''

    if (!ref)
        return null

    let clientState = await (await fetch(`${HEXA_BACKUP_BASE_URL}/refs/${ref}`)).json()
    if (!clientState)
        return null

    let commitSha = clientState.currentCommitSha
    let firstDirectoryDescriptorSha = null

    let finishLoading = startLoading(`loading commit history`)

    while (commitSha != null) {
        let commit = await fetchCommit(commitSha)
        if (!commit)
            break

        let date = new Date(commit.commitDate)
        let directoryDescriptorSha = commit.directoryDescriptorSha
        if (directoryDescriptorSha) {
            if (!firstDirectoryDescriptorSha)
                firstDirectoryDescriptorSha = directoryDescriptorSha
            el('#commitHistory').innerHTML += `<div>${commitSha.substr(0, 7)} ${displayDate(date)} - <a href='#' onclick='event.preventDefault() || goDirectory("${directoryDescriptorSha}")'>${directoryDescriptorSha.substr(0, 7)}</a></div>`
        } else {
            el('#commitHistory').innerHTML += `<div>${commitSha.substr(0, 7)} no directory descriptor in commit !</div>`
        }

        if (!SHOW_FULL_COMMIT_HISTORY)
            break

        commitSha = commit.parentSha
    }

    finishLoading()

    return firstDirectoryDescriptorSha
}

async function fetchCommit(sha) {
    let mimeType = 'text/json'
    let content = await fetch(`${HEXA_BACKUP_BASE_URL}/sha/${sha}/content?type=${mimeType}`)
    return await content.json()
}

async function fetchDirectoryDescriptor(sha) {
    let mimeType = 'text/json'
    let content = await fetch(`${HEXA_BACKUP_BASE_URL}/sha/${sha}/content?type=${mimeType}`)
    return await content.json()
}

function infiniteScroll(db, domCreator, scrollContainer, scrollContent) {
    let nextElementToInsert = 0
    let poolSize = 5
    let stopped = false
    let waitContinueResolver = null

    async function run() {
        if (!db || !db.length)
            return

        const shouldAdd = () => scrollContent.lastChild.offsetTop - (scrollContainer.offsetHeight / 1) <= scrollContainer.scrollTop + scrollContainer.offsetHeight

        const scrollListener = event => {
            if (waitContinueResolver && shouldAdd()) {
                let r = waitContinueResolver
                waitContinueResolver = null
                r()
            }
        }
        scrollContainer.addEventListener('scroll', scrollListener)

        stopped = false
        while (!stopped) {
            if (nextElementToInsert >= db.length)
                break

            let index = nextElementToInsert++
            let elem = db[index]

            scrollContent.innerHTML += domCreator(elem, index)

            if (nextElementToInsert % poolSize == 0) {
                await wait(150)

                if (!shouldAdd())
                    await new Promise(resolve => waitContinueResolver = resolve)
            }
        }

        scrollContainer.removeEventListener('scroll', scrollListener)
    }

    function stop() {
        stopped = true
        if (waitContinueResolver) {
            waitContinueResolver()
            waitContinueResolver = null
        }
    }

    run()

    return stop
}

window.addEventListener('load', async () => {
    let resp = await fetch(`${HEXA_BACKUP_BASE_URL}/refs`)
    let refs = (await resp.json()).filter(e => e.startsWith('CLIENT_')).map(e => e.substr(7))
    el('#refs-list').innerHTML = refs.map(ref => `<a href='#' onclick='event.preventDefault() || goRef("${ref}")'>${ref}</a>`).join('<br/>')
})

el('#fullScreen').addEventListener('click', () => {
    el('body').webkitRequestFullScreen()
})

el('#extended').addEventListener('change', () => {
    EXTENDED = !!el('#extended').checked
    localStorage.setItem('EXTENDED', `${EXTENDED}`)

    syncUi()
})

el('#stream-raw-video').addEventListener('change', () => {
    STREAM_RAW_VIDEO = !!el('#stream-raw-video').checked
    localStorage.setItem('STREAM_RAW_VIDEO', `${STREAM_RAW_VIDEO}`)

    syncUi()
})

el('#show-unliked-items').addEventListener('change', () => {
    SHOW_UNLIKED_ITEMS = !!el('#show-unliked-items').checked
    localStorage.setItem('SHOW_UNLIKED_ITEMS', `${SHOW_UNLIKED_ITEMS}`)

    syncUi()
})

el('#show-full-commit-history').addEventListener('change', () => {
    SHOW_FULL_COMMIT_HISTORY = !!el('#show-full-commit-history').checked
    localStorage.setItem('SHOW_FULL_COMMIT_HISTORY', `${SHOW_FULL_COMMIT_HISTORY}`)

    syncUi()
})

el('#audio-player').addEventListener('ended', () => {
    listenNext()
})

el('#video-player').addEventListener('ended', () => {
    showNextVideo()
})

window.onpopstate = function (event) {
    if (event.state) {
        currentDirectoryDescriptorSha = event.state.currentDirectoryDescriptorSha
        currentClientId = event.state.currentClientId
        currentPictureIndex = event.state.currentPictureIndex || 0

        if (!currentClientId)
            el("#menu").classList.remove("hide-optional")

        syncUi()
    }
    else {
        fromHash()

        syncUi()
    }
}

if (history.state) {
    currentDirectoryDescriptorSha = history.state.currentDirectoryDescriptorSha
    currentClientId = history.state.currentClientId
    currentPictureIndex = history.state.currentPictureIndex || 0

    if (currentClientId)
        el("#menu").classList.add("hide-optional")

    syncUi()
}
else {
    fromHash()

    publishHistoryState()

    if (window.location.hash && window.location.hash.startsWith('#') && window.location.hash.length > 10 && window.location.hash != '#null')
        el('#optional').classList.add('optional')

    syncUi()
}

function fromHash() {
    if (window.location.hash && window.location.hash.startsWith('#') && window.location.hash != '#null' && window.location.hash != '#undefined') {
        currentDirectoryDescriptorSha = window.location.hash.substr(1)
        currentClientId = null

        currentPictureIndex = -1
        let dashIndex = currentDirectoryDescriptorSha.indexOf('-')
        if (dashIndex >= 0) {
            currentPictureIndex = parseInt(currentDirectoryDescriptorSha.substr(dashIndex + 1))
            currentDirectoryDescriptorSha = currentDirectoryDescriptorSha.substr(0, dashIndex)
        }

        el("#menu").classList.add("hide-optional")
    }
}

async function viewLikes() {
    el("#menu").classList.remove("hide-optional")

    let likes = await (await fetch(`/metadata/likes-sha`)).json()
    if (!likes)
        likes = {}

    // TODO manage liked directories
    el('#directories').classList.add('is-hidden')
    el('#videos-container').classList.remove('is-hidden')
    el('#audio-container').classList.remove('is-hidden')
    // TODO manage liked images
    el('#images-container').classList.add('is-hidden')

    let likesArray = Object.getOwnPropertyNames(likes).map(sha => ({ sha, value: likes[sha] }))

    if (!SHOW_UNLIKED_ITEMS)
        likesArray = likesArray.filter(like => like.value.status)

    await viewLikedFiles(likesArray)
    await listenToLiked(likesArray.filter(like => like.value.knownAs.mimeType.startsWith('audio/')))
    await viewLikedVideos(likesArray.filter(like => like.value.knownAs.mimeType.startsWith('video/')))
}

async function syncUi() {
    if (currentPictureIndex < 0) {
        el('#image-full-container').classList.add('is-hidden')
        el('#images-container').classList.remove('is-hidden')
    }
    else {
        el('#image-full-container').classList.remove('is-hidden')
        el('#images-container').classList.add('is-hidden')
    }

    const extChange = displayedExtended != EXTENDED
    displayedExtended = EXTENDED

    const streamRawVideoChange = displayedStreamRawVideo != STREAM_RAW_VIDEO
    displayedStreamRawVideo = STREAM_RAW_VIDEO

    const fullHistoryChange = displayedShowFullCommitHistory != SHOW_FULL_COMMIT_HISTORY
    displayedShowFullCommitHistory = SHOW_FULL_COMMIT_HISTORY

    const showUnlikedItemsChange = displayedShowUnlikedItems != SHOW_UNLIKED_ITEMS
    displayedShowUnlikedItems = SHOW_UNLIKED_ITEMS

    if (showUnlikedItemsChange || streamRawVideoChange || extChange || currentDirectoryDescriptorSha != displayedDirectoryDescriptorSha)
        await showDirectory(currentDirectoryDescriptorSha)

    if (showUnlikedItemsChange || extChange || fullHistoryChange || currentClientId != displayedClientId)
        await showRef(currentClientId)

    if (videosPool.length)
        el('#videos-container').classList.remove('is-hidden')
    else
        el('#videos-container').classList.add('is-hidden')

    if (audioPool.length)
        el('#audio-container').classList.remove('is-hidden')
    else
        el('#audio-container').classList.add('is-hidden')

    if (!imagesPool.length)
        el('#images-container').classList.add('is-hidden')

    if (currentPictureIndex != displayedPictureIndex)
        await showPicture(currentPictureIndex)

    displayedDirectoryDescriptorSha = currentDirectoryDescriptorSha
    displayedClientId = currentClientId
    displayedPictureIndex = currentPictureIndex
}