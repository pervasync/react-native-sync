import RNFetchBlob from 'react-native-fetch-blob'
import util from './util.js'

var PervasyncDir = RNFetchBlob.fs.dirs.DocumentDir + "/pervasync";

async function rm(path) {
    return await RNFetchBlob.fs.unlink(path);
}

async function rmrf(path) {
    console.log("fs rmrf, path=" + path);

    let exists = await RNFetchBlob.fs.exists(path);
    console.log("exists=" + exists);
    if (!exists) {
        return;
    }
    let isDir = await RNFetchBlob.fs.isDir(path);
    console.log("isDir=" + isDir);
    if (isDir) {
        let files = await RNFetchBlob.fs.ls(path);
        for (let i = 0; i < files.length; i++) {
            let file = files[i];
            let filePath = path + "/" + file;
            console.log("filePath=" + filePath);
            await rmrf(filePath);
        }
    }
    return await RNFetchBlob.fs.unlink(path);
}

async function createFile(path) {
    return await RNFetchBlob.fs.createFile(path, "", 'utf8');
}

async function writeString(path, str) {
    return await RNFetchBlob.fs.writeFile(path, str, 'utf8');
}

/**
 * 
 * @param {*} path 
 * @param {*} bytes Uint8Array
 */
async function writeBytes(path, bytes) {
    console.log("begin writeBytes, path=" + path);

    // base64 encode
    console.log("encode Uint8Array bytes to base64Str");
    let base64Str = util.bytesToBase64(bytes);
    console.log("Calling RNFetchBlob.fs.writeFile");
    let result = await RNFetchBlob.fs.writeFile(path, base64Str, 'base64');
    /* 
        let array = Array.from(bytes);
        console.log("Calling RNFetchBlob.fs.writeFile");
        let result = await RNFetchBlob.fs.writeFile(path, array, 'ascii');// too slow
        */
    console.log("end writeBytes");
    return result;
}

async function readString(path) {
    let str = await RNFetchBlob.fs.readFile(path, 'utf8');
    return str;
}

/**
 * 
 * @param {*} path 
 * @returns Uint8Array
 */
async function readBytes(path) {
    let array = await RNFetchBlob.fs.readFile(path, 'ascii');
    console.log("readBytes, array.length=" + array.length);
    let bytes = Uint8Array.from(array);
    console.log("readBytes, bytes.length=" + bytes.length);
    return bytes;
}

async function mkdir(path) {
    return await RNFetchBlob.fs.mkdir(path);
}

async function mkdirs(path) {
    let parentPath = parent(path);
    let parentExist = await RNFetchBlob.fs.exists(parentPath);
    if (!parentExist) {
        await mkdirs(parentPath);
    }
    return await RNFetchBlob.fs.mkdir(path);
}

function parent(path) {
    return path.substring(0, path.lastIndexOf("/"));
}
async function ls(path) {
    return await RNFetchBlob.fs.ls(path);
}

async function mv(from, to) {
    return await RNFetchBlob.fs.mv(from, to);
}

async function cp(src, dest) {
    return await RNFetchBlob.fs.cp(src, dest);
}

async function exists(path) {
    return await RNFetchBlob.fs.exists(path);
}

async function isDir(path) {
    return await RNFetchBlob.fs.isDir(path);
}

async function stat(path) {
    return await RNFetchBlob.fs.stat(path);
}

export default {
    PervasyncDir,
    rm,
    rmrf,
    createFile,
    writeString,
    writeBytes,
    readString,
    readBytes,
    mkdir,
    mkdirs,
    ls,
    mv,
    cp,
    exists,
    isDir,
    stat,
    parent
}