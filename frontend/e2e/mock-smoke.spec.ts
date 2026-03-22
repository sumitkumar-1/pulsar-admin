import { expect, test } from '@playwright/test';

test.describe('mock smoke @mock', () => {
  test('navigates environments, explorer, topic details, and namespace yaml in mock mode', async ({ page }) => {
    await page.goto('/environments?mode=mock');

    await expect(page.getByRole('heading', { name: 'Connected environments' })).toBeVisible();
    await expect(page.getByText('Production')).toBeVisible();

    await page.getByRole('link', { name: /production/i }).click();

    await expect(page.getByRole('heading', { name: 'Start with a namespace.' })).toBeVisible();
    await page.getByRole('button', { name: /acme\/analytics|public\/default|smtp\/test/i }).first().click();

    await expect(page.getByRole('button', { name: 'Topics' })).toBeVisible();
    await page.getByRole('button', { name: 'Namespace' }).click();
    await expect(page.getByRole('button', { name: /Open Namespace YAML/i })).toBeVisible();
    await page.getByRole('button', { name: /Open Namespace YAML/i }).click();

    await expect(page.getByRole('heading', { name: /Namespace YAML editor/i })).toBeVisible();
    await expect(page.locator('.cm-content')).toBeVisible();
    await page.getByRole('button', { name: /Preview Changes/i }).click();
    await expect(page.getByText(/Review Changes|Validation needs attention|YAML passed validation/i)).toBeVisible();

    await page.getByRole('link', { name: /Back to topics/i }).click();
    await page.getByRole('button', { name: 'Topics' }).click();
    await page.locator('.topic-card').first().click();

    await expect(page.getByRole('button', { name: 'Overview' })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Operations' })).toBeVisible();
  });
});
