import * as https from 'https'

export function post(url: string, data: any, headers: {}): Promise<{ statusCode: number; body: any }> {
    return new Promise((resolve, reject) => {
        const options = {
            method: 'POST',
            headers
        }

        const req = https.request(url, options, resp => {
            let data = ''
            resp.on('data', chunk => data += chunk)
            resp.on('end', () => resolve({ statusCode: resp.statusCode, body: data }))
        })

        req.on('error', err => {
            reject(err)
        })

        if (data)
            req.write(data)

        req.end()
    })
}