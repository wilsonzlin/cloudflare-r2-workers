import assertExists from "@xtjs/lib/js/assertExists";
import mapDefined from "@xtjs/lib/js/mapDefined";
import mapValue from "@xtjs/lib/js/mapValue";
import parseRangeHeader from "@xtjs/lib/js/parseRangeHeader";

export const handleHeadOrGetOfObject = async ({
  additionalHeaders,
  request,
  bucket,
  key,
  contentDisposition,
  contentType,
}: {
  // We don't automatically set CORS headers as the Worker may perform other tasks (e.g. POST).
  additionalHeaders?: { [name: string]: string };
  request: {
    method: string;
    headers: Headers;
  };
  bucket: R2Bucket;
  key: string;
  contentDisposition?: string;
  contentType?: string;
}) => {
  if (request.method === "HEAD" || request.method === "GET") {
    const rangeRaw = request.headers.get("range");
    const range = parseRangeHeader(rangeRaw ?? "");
    if (rangeRaw && !range) {
      return new Response("Range Not Satisfiable", { status: 416 });
    }

    const object = await bucket.get(key, {
      range: mapDefined(range, ({ start, end }) => ({
        offset: start,
        length: end == undefined ? undefined : end - start + 1,
      })),
    });

    if (object === null || !("body" in object)) {
      return new Response("Not Found", { status: 404 });
    }

    const responseHeaders = new Headers();
    object.writeHttpMetadata(responseHeaders);
    responseHeaders.set("etag", object.httpEtag);
    if (contentDisposition != undefined) {
      responseHeaders.set("content-disposition", contentDisposition);
    }
    if (contentType != undefined) {
      responseHeaders.set("content-type", contentType);
    }

    // WARNING: `object.range` may still be set even if we didn't request a range, so don't use it's existence to determine whether to respond with 206 and Content-Range.
    const contentLength =
      mapDefined(object.range, (r) => r["length"]) ?? object.size;
    const isPartial = !!object.range && object.range["length"] != object.size;
    responseHeaders.set("content-length", contentLength.toString());

    return new Response(request.method === "HEAD" ? undefined : object.body, {
      status: isPartial ? 206 : 200,
      headers: {
        "accept-ranges": "bytes",
        ...additionalHeaders,
        ...Object.fromEntries(responseHeaders),
        ...(!isPartial
          ? {}
          : mapValue(object.range!, (r) => ({
              "content-range": `bytes ${assertExists(r["offset"])}-${
                r["offset"] + assertExists(r["length"]) - 1
              }/${object.size}`,
            })) ?? {}),
      },
    });
  }

  return new Response("Method Not Allowed", {
    status: 405,
    headers: {
      Allow: "GET",
    },
  });
};
