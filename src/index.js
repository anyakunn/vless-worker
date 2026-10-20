export default {
  async fetch(request, env, ctx) {
    return new Response("Hello from Worker! It is working.", {
      status: 200,
      headers: { "Content-Type": "text/plain" },
    });
  },
};
