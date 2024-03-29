const DATE_DISPLAY_OPTIONS = {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
}

export const displayDate = (date: number | Date) => (typeof date === 'number' ? new Date(date) : date).toLocaleString('fr', DATE_DISPLAY_OPTIONS as Intl.DateTimeFormatOptions)

export function prettySize(size: number): string {
    if (size < 1024)
        return size.toString() + ' bytes'
    if (size < 1024 * 1024)
        return (size / 1024).toFixed(2) + ' kB'
    if (size < 1024 * 1024 * 1024)
        return (size / (1024 * 1024)).toFixed(3) + ' MB'
    return (size / (1024 * 1024 * 1024)).toFixed(3) + ' GB'
}

export function prettySpeed(size: number, time: number) {
    if (time <= 0)
        return '-'
    return `${prettySize((1000 * size) / (time))}/s`
}

export function prettyTime(time: number): string {
    if (time < 1000)
        return Math.floor(time) + ' ms'
    if (time < 1000 * 60)
        return (time / 1000).toFixed(2) + ' s'
    if (time < 1000 * 60 * 60)
        return (time / (1000 * 60)).toFixed(2) + ' minutes'
    return (time / (1000 * 60 * 60)).toFixed(2) + ' hours'
}