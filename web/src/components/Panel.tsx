export function Panel({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="panel min-w-0 p-4 sm:p-5">
      <h2 className="serif mb-3 text-lg font-semibold">{title}</h2>
      {children}
    </section>
  )
}
