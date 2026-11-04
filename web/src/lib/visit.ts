export type DemoStep = 'broke-family' | 'asked-don'

export function reportStep(step: DemoStep): void {
  document.dispatchEvent(new CustomEvent('najafov:action', { detail: step }))
}
