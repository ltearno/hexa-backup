const DATE_DISPLAY_OPTIONS = {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
}

export const displayDate = (date: number | Date) => (typeof date === 'number' ? new Date(date) : date).toLocaleString('fr', DATE_DISPLAY_OPTIONS)

export function prettySize(size: number): string {
    if (size < 1024)
        return size.toString() + ' bytes'
    if (size < 1024 * 1024)
        return (size / 1024).toFixed(2) + ' kB'
    if (size < 1024 * 1024 * 1024)
        return (size / (1024 * 1024)).toFixed(3) + ' MB'
    return (size / (1024 * 1024 * 1024)).toFixed(3) + ' GB'
}