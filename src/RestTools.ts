import * as https from 'https'
import * as http from 'http'

export function post(url: string, data: any, headers: {}): Promise<{ statusCode: number; body: any }> {
    return new Promise((resolve, reject) => {
        const req = https.request(url, {
            headers,
            method: 'POST'
        }, resp => {
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